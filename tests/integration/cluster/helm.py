import os
import socket
import subprocess
import uuid
from os import getenv
from time import sleep

from tests.integration.cluster.base import AppManager, get_free_port

CORE_HELM_CHART_VERSION_ENV = "CORE_HELM_CHART_VERSION"
CORE_DEFAULT_HELM_CHART_VERSION = "0.3.0"
CORE_IMAGE_TAG_ENV = "CORE_IMAGE_TAG"
CORE_PORT = 3473


class HelmAppManager(AppManager):
    def __init__(self, k8s_cluster):
        self.k8s_cluster = k8s_cluster

    def deploy(self, params=None):
        return deploy_helm_chart(self.k8s_cluster, helm_values=params)

    def cleanup(self, setup_data):
        cleanup_helm_chart(self.k8s_cluster, setup_data)


def deploy_helm_chart(k8s_cluster, helm_values=None):
    """Common logic for both session and function-scoped setups."""
    test_id = (
        f"{os.environ.get('PYTEST_XDIST_WORKER', 'python-sdk')}-{uuid.uuid4().hex[:4]}"
    )
    release, ns = f"core-{test_id}", f"ns-{test_id}"
    local_port = get_free_port()

    # Use CORE_IMAGE_TAG if not provided in helm_values
    if helm_values is None:
        helm_values = {}
    if "image.tag" not in helm_values:
        core_image_tag = getenv(CORE_IMAGE_TAG_ENV)
        if core_image_tag:
            helm_values["image.tag"] = core_image_tag

    set_args = []
    if helm_values:
        for key, value in helm_values.items():
            set_args.extend(["--set", f"{key}={value}"])

    print(f"[Kind] Installing Helm release {release} into namespace {ns}...")
    subprocess.run(
        [
            "helm",
            "install",
            release,
            "oci://ghcr.io/firebolt-db/helm-charts/firebolt-core",
            "--version",
            getenv(CORE_HELM_CHART_VERSION_ENV, CORE_DEFAULT_HELM_CHART_VERSION),
            "-n",
            ns,
            "--create-namespace",
            "--wait",
            "--kube-context",
            k8s_cluster,
        ]
        + set_args,
        check=True,
    )

    print(f"[Kind] Waiting for pods in {ns} to be ready...")
    subprocess.run(
        [
            "kubectl",
            "wait",
            "--for=condition=ready",
            "pod",
            "-l",
            "app.kubernetes.io/instance=" + release,
            "--namespace",
            ns,
            "--timeout=120s",
            "--context",
            k8s_cluster,
        ],
        check=True,
    )

    pod_names_result = subprocess.run(
        [
            "kubectl",
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/instance=" + release,
            "-n",
            ns,
            "-o",
            "jsonpath={.items[*].metadata.name}",
            "--context",
            k8s_cluster,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    pod_names = pod_names_result.stdout.split()

    pf_procs = []
    ips_with_ports = []
    for i, pod_name in enumerate(pod_names):
        ip = "127.0.0.1"
        port = local_port + i
        ips_with_ports.append(f"{ip}:{port}")
        print(f"[Kind] Port-forward to pod {pod_name} on {ip}:{port}->{CORE_PORT}...")
        pf_proc = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                "--address",
                ip,
                f"pod/{pod_name}",
                f"{port}:{CORE_PORT}",
                "-n",
                ns,
                "--context",
                k8s_cluster,
            ]
        )
        pf_procs.append(pf_proc)

    sleep(1)
    # Wait for port-forward
    for i in range(10):
        try:
            # Check all port-forwards
            all_up = True
            for ip_port in ips_with_ports:
                ip, port = ip_port.split(":")
                try:
                    with socket.create_connection((ip, int(port)), timeout=1):
                        print(f"[Kind] Port-forward on {ip}:{port} is UP")
                except (socket.error, ConnectionRefusedError):
                    all_up = False
                    break
            if all_up:
                break
        except (socket.error, ConnectionRefusedError):
            if i == 9:
                raise RuntimeError(f"Failed to connect to port-forward on {local_port}")
            sleep(1)

    url = f"http://127.0.0.1:{local_port}"

    # Return everything needed for cleanup
    return {
        "url": url,
        "processes": pf_procs,
        "release": release,
        "ns": ns,
        "ips": ips_with_ports,
    }


def cleanup_helm_chart(k8s_cluster, setup_data):
    """Common teardown logic."""
    for proc in setup_data["processes"]:
        proc.terminate()
    subprocess.run(
        [
            "helm",
            "uninstall",
            setup_data["release"],
            "-n",
            setup_data["ns"],
            "--kube-context",
            k8s_cluster,
        ],
        check=True,
    )
    subprocess.run(["kubectl", "delete", "ns", setup_data["ns"]], check=True)
