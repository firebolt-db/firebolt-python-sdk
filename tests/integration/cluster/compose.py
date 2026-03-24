import json
import os
import shutil
import socket
import subprocess
import tempfile
import uuid
from os import getenv
from time import sleep

import yaml

from tests.integration.cluster.base import AppManager, get_free_port

NGINX_CONFIG_TEMPLATE = """
{upstream_block}
server {{
    listen 443 ssl;
    server_name localhost 127.0.0.1;

    ssl_certificate     /etc/nginx/certs/server.pem;
    ssl_certificate_key /etc/nginx/certs/server.key;

    location / {{
        proxy_pass http://{proxy_target};
        proxy_set_header Host $host;
    }}
}}
"""


def generate_self_signed_cert(cert_path: str, key_path: str) -> None:
    """Generate a self-signed certificate for localhost using openssl."""
    os.makedirs(os.path.dirname(cert_path), exist_ok=True)
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:4096",
            "-keyout",
            key_path,
            "-out",
            cert_path,
            "-days",
            "365",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName = DNS:localhost, IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )


class ComposeAppManager(AppManager):
    def deploy(self, params=None):
        return deploy_compose(params)

    def cleanup(self, setup_data):
        cleanup_compose(setup_data)


def deploy_compose(params=None):
    """Deploy Firebolt Core using Docker Compose."""
    if params is None:
        params = {}

    nodes_count = int(params.get("nodesCount", 1))
    image_tag = params.get("image.tag", getenv("CORE_IMAGE_TAG", "preview-rc"))

    test_id = (
        f"{os.environ.get('PYTEST_XDIST_WORKER', 'python-sdk')}-{uuid.uuid4().hex[:4]}"
    )
    project_name = f"core-{test_id}"

    tmp_dir = tempfile.mkdtemp(prefix=f"firebolt-compose-{test_id}")
    resources_dir = os.path.join(tmp_dir, "resources")
    certs_dir = os.path.join(resources_dir, "certs")
    os.makedirs(certs_dir, exist_ok=True)

    # Generate certs
    server_cert_path = os.path.join(certs_dir, "server.pem")
    server_key_path = os.path.join(certs_dir, "server.key")
    generate_self_signed_cert(server_cert_path, server_key_path)

    # Generate nodes
    node_names = [f"firebolt-core-{i}" for i in range(nodes_count)]

    # Generate core config
    core_config = {"nodes": [{"host": name} for name in node_names]}
    with open(os.path.join(resources_dir, "config.json"), "w") as f:
        json.dump(core_config, f, indent=2)

    # Generate nginx config
    if nodes_count > 1:
        upstream_servers = "".join([f"server {name}:3473; " for name in node_names])
        upstream_block = f"upstream firebolt {{ {upstream_servers}}}"
        proxy_target = "firebolt"  # Upstream name, no port needed
    else:
        # Single node, no upstream block needed
        upstream_block = ""
        proxy_target = f"{node_names[0]}:3473"

    nginx_config = NGINX_CONFIG_TEMPLATE.format(
        upstream_block=upstream_block, proxy_target=proxy_target
    )
    with open(os.path.join(resources_dir, "default.conf"), "w") as f:
        f.write(nginx_config)

    # Generate docker-compose.yaml
    node_ports = []
    services = {}

    for i, name in enumerate(node_names):
        node_port = get_free_port()
        node_ports.append(node_port)
        services[name] = {
            "image": f"ghcr.io/firebolt-db/firebolt-core:{image_tag}",
            "container_name": f"{project_name}-{name}",
            "command": f"--node {i}",
            "privileged": True,
            "restart": "no",
            "ulimits": {"memlock": 8589934592},
            "ports": [f"{node_port}:3473"],
            "volumes": [
                "./resources/config.json:/firebolt-core/config.json:ro",
                f"./{name}:/firebolt-core/data",
            ],
        }
        # Create data dir
        os.makedirs(os.path.join(tmp_dir, name), exist_ok=True)

    # Create one nginx instance per core node, all load balancing
    nginx_ports = []
    for i in range(nodes_count):
        nginx_port = get_free_port()
        nginx_ports.append(nginx_port)
        services[f"nginx-{i}"] = {
            "image": "nginx:alpine",
            "container_name": f"{project_name}-nginx-{i}",
            "ports": [f"{nginx_port}:443"],
            "volumes": [
                "./resources/certs:/etc/nginx/certs:ro",
                "./resources/default.conf:/etc/nginx/conf.d/default.conf:ro",
            ],
            "depends_on": node_names,
        }

    compose_data = {"services": services}
    with open(os.path.join(tmp_dir, "docker-compose.yaml"), "w") as f:
        yaml.dump(compose_data, f, default_flow_style=False)

    print(f"[Compose] Starting project {project_name} in {tmp_dir}...")
    subprocess.run(
        ["docker", "compose", "-p", project_name, "up", "-d"],
        cwd=tmp_dir,
        check=True,
        capture_output=True,
    )

    # Wait for core to be healthy
    print(f"[Compose] Waiting for cluster {project_name} to be healthy...")
    for i in range(30):
        try:
            # Try to connect to the core port directly
            with socket.create_connection(("127.0.0.1", node_ports[0]), timeout=1):
                res = subprocess.run(
                    [
                        "curl",
                        "-s",
                        "-o",
                        "/dev/null",
                        "-w",
                        "%{http_code}",
                        f"http://127.0.0.1:{node_ports[0]}",
                    ],
                    capture_output=True,
                    text=True,
                )
                if res.stdout.strip() == "200":
                    print(f"[Compose] Cluster is healthy at 127.0.0.1:{node_ports[0]}")
                    break
        except (socket.error, ConnectionRefusedError):
            pass

        if i == 29:
            raise RuntimeError(
                f"Cluster {project_name} failed to become healthy at {node_ports[0]}"
            )
        sleep(2)

    ips = [f"127.0.0.1:{port}" for port in node_ports]
    url = f"http://127.0.0.1:{node_ports[0]}"

    return {
        "url": url,
        "project_name": project_name,
        "tmp_dir": tmp_dir,
        "ips": ips,
        "nginx_ports": nginx_ports,  # list of ports
        "server_cert_path": server_cert_path,
    }


def cleanup_compose(setup_data):
    """Stop and clean up Docker Compose project."""
    print(f"[Compose] Stopping project {setup_data['project_name']}...")
    subprocess.run(
        ["docker", "compose", "-p", setup_data["project_name"], "down", "-v"],
        cwd=setup_data["tmp_dir"],
        check=True,
        capture_output=True,
    )
    shutil.rmtree(setup_data["tmp_dir"])
