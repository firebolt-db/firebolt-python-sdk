from firebolt.firebolt_client import FireboltClient
from firebolt.model.region import regions


def test_region(mocked_api, settings, mock_regions):
    with FireboltClient(settings=settings):
        assert regions.regions == mock_regions
        assert mocked_api["regions"].called
