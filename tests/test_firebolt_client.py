from firebolt.firebolt_client import FireboltClient


def test_settings(mocked_api, settings):
    with FireboltClient(settings=settings) as fc:
        assert fc.settings == settings


def test_auth(mocked_api, settings, access_token):
    with FireboltClient(settings=settings) as fc:
        assert mocked_api["auth"].called
        assert fc.access_token == access_token


def test_account_id(mocked_api, settings, account_id):
    with FireboltClient(settings=settings) as fc:
        assert fc.account_id == account_id
        assert mocked_api["account_id"].called
