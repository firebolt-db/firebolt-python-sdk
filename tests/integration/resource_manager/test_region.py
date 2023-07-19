from firebolt.service.manager import ResourceManager


def test_get_region():
    rm = ResourceManager()
    assert len(rm.regions.regions) == 1, "Invalid number of regions returned"
