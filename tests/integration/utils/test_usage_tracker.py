import os
from pathlib import Path
from shutil import rmtree
from subprocess import PIPE, run

from pytest import fixture, mark

TEST_FOLDER = "tmp_test_code/"
TEST_SCRIPT_MODEL = "tests/integration/utils/sample_usage.model"


MOCK_MODULES = [
    "firebolt_cli/firebolt_cli.py",
    "sqlalchemy/engine/firebolt_db.py",
    "firebolt_provider/hooks/firebolt_provider.py",
    "dbt/adapters/firebolt/dbt/adapters/firebolt.py",
]


@fixture(scope="module", autouse=True)
def create_cli_mock():
    for i, file in enumerate(MOCK_MODULES):
        os.makedirs(os.path.dirname(f"{TEST_FOLDER}{file}"))
        with open(f"{TEST_FOLDER}{file}", "w") as f:
            f.write(f"__version__ = '1.0.{i}'")
    # Additional setup for proper dbt import
    Path(f"{TEST_FOLDER}dbt/adapters/firebolt/dbt/__init__.py").touch()
    Path(f"{TEST_FOLDER}/dbt/adapters/firebolt/dbt/adapters/__init__.py").touch()
    yield
    rmtree(TEST_FOLDER)


@fixture(scope="module")
def test_model():
    with open(TEST_SCRIPT_MODEL) as f:
        return f.read()


def create_test_file(code: str, function_name: str, file_path: str):
    code = code.format(function_name=function_name)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write(code)


@mark.parametrize(
    "function,path,expected",
    [
        ("create_connection", "firebolt_cli/utils.py", "FireboltCLI/1.0.0"),
        ("connect", "sqlalchemy/engine/default.py", "SQLAlchemy/1.0.1"),
        ("establish_connection", "source_firebolt/source.py", "AirbyteSource/"),
        ("establish_async_connection", "source_firebolt/source.py", "AirbyteSource/"),
        (
            "establish_connection",
            "destination_firebolt/destination.py",
            "AirbyteDestination/",
        ),
        (
            "establish_async_connection",
            "destination_firebolt/destination.py",
            "AirbyteDestination/",
        ),
        ("get_conn", "firebolt_provider/hooks/firebolt.py", "Airflow/1.0.2"),
        ("open", "dbt/adapters/firebolt/connections.py", "DBT/1.0.3"),
    ],
)
def test_usage_detection(function, path, expected, test_model):
    test_path = TEST_FOLDER + path
    create_test_file(test_model, function, test_path)
    result = run(
        ["python3", test_path],
        stdout=PIPE,
        stderr=PIPE,
        env={"PYTHONPATH": os.getenv("PYTHONPATH")},
    )
    assert not result.stderr
    assert expected in result.stdout.decode("utf-8")
