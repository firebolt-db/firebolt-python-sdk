from pytest import fixture


@fixture
def multi_param_engine_name(database: str) -> str:
    return f"{database}_multi_param"


@fixture
def single_param_engine_name(database: str) -> str:
    return f"{database}_single_param"


@fixture
def start_stop_engine_name(database: str) -> str:
    return f"{database}_start_stop"
