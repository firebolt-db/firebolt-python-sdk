from pyfakefs.fake_filesystem_unittest import Patcher
from pytest import fixture


@fixture(autouse=True)
def global_fake_fs() -> None:
    with Patcher():
        yield
