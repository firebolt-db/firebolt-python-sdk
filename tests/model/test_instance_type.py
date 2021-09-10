from firebolt.firebolt_client import FireboltClient
from firebolt.model.instance_type import instance_types


def test_instance_type(mocked_api, settings, mock_instance_types):
    with FireboltClient(settings=settings):
        assert instance_types.instance_types == mock_instance_types
        assert mocked_api["instance_types"].called
