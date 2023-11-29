from pytest import fixture


@fixture
def multi_param_engine_name(database_name: str) -> str:
    return f"{database_name}_multi_param"


@fixture
def single_param_engine_name(database_name: str) -> str:
    return f"{database_name}_single_param"


@fixture
def start_stop_engine_name(database_name: str) -> str:
    return f"{database_name}_start_stop"
