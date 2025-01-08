import time

from pytest import raises

from firebolt.utils.exception import QueryTimeoutError
from firebolt.utils.timeout_controller import TimeoutController


def test_timeout_controller():
    timeout = 1
    controller = TimeoutController(timeout)
    remaining = controller.remaining()
    assert 1 > remaining > 0

    controller.check_timeout()
    time.sleep(1)
    assert controller.remaining() == 0
    with raises(QueryTimeoutError):
        controller.check_timeout()


def test_timeout_controller_no_timeout():
    timeout = None
    controller = TimeoutController(timeout)
    assert controller.remaining() is None
    controller.check_timeout()
