from firebolt.firebolt_client import FireboltClient
from firebolt.model.provider import providers


def test_provider(mocked_api, settings, mock_providers):
    with FireboltClient(settings=settings):
        assert providers.providers == mock_providers
        assert mocked_api["providers"].called
