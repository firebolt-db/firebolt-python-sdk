from pytest import fixture, raises

from firebolt.utils.exception import FireboltStructuredError


@fixture
def error_1():
    return {
        "code": "123",
        "name": "Test Error",
        "severity": "High",
        "source": "Test Source",
        "description": "Test Description",
        "resolution": "Test Resolution",
        "helpLink": "www.test.com",
        "location": {"failingLine": 1, "startOffset": 1, "endOffset": 1},
    }


@fixture
def error_2():
    return {
        "code": "456",
        "name": "Test Error 2",
        "severity": "Low",
        "source": "Test Source 2",
        "description": "Test Description 2",
        "resolution": "Test Resolution 2",
        "helpLink": "www.test2.com",
        "location": {"failingLine": 2, "startOffset": 2, "endOffset": 2},
    }


@fixture
def error_missing_fields():
    return {
        "name": "Test Error 2",
        "source": "Test Source 2",
        "description": "Test Description 2",
        "resolution": "Test Resolution 2",
    }


def test_firebolt_structured_error(error_1):
    error_json = {"errors": [error_1]}

    error = FireboltStructuredError(error_json)

    assert len(error.errors) == 1

    assert (
        str(error)
        == "High: Test Error (123) - Test Description at {'failingLine': 1, 'startOffset': 1, 'endOffset': 1}, see www.test.com"
    )


def test_firebolt_structured_error_multiple(error_1, error_2):
    error_json = {"errors": [error_1, error_2]}

    error = FireboltStructuredError(error_json)

    assert len(error.errors) == 2

    assert str(error) == (
        "High: Test Error (123) - Test Description at {'failingLine': 1, 'startOffset': 1, 'endOffset': 1}, see www.test.com,\n"
        "Low: Test Error 2 (456) - Test Description 2 at {'failingLine': 2, 'startOffset': 2, 'endOffset': 2}, see www.test2.com"
    )


def test_firebolt_structured_error_missing_fields(error_1, error_missing_fields):
    error_json = {"errors": [error_1, error_missing_fields]}

    error = FireboltStructuredError(error_json)

    assert len(error.errors) == 2

    assert str(error) == (
        "High: Test Error (123) - Test Description at {'failingLine': 1, 'startOffset': 1, 'endOffset': 1}, see www.test.com,\n"
        "Test Error 2 - Test Description 2"
    )


def test_firebolt_structured_error_empty():
    error_json = {}

    error = FireboltStructuredError(error_json)

    assert len(error.errors) == 0


def test_error_when_raised(error_1):
    error_json = {"errors": [error_1]}

    with raises(Exception) as e:
        raise FireboltStructuredError(error_json)

    assert (
        str(e.value)
        == "High: Test Error (123) - Test Description at {'failingLine': 1, 'startOffset': 1, 'endOffset': 1}, see www.test.com"
    )
