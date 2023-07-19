from firebolt.service.manager import ResourceManager, Settings


def test_get_region(rm_settings: Settings):
    rm = ResourceManager(rm_settings)
    assert len(rm.regions.regions) == 1, "Invalid number of regions returned"
