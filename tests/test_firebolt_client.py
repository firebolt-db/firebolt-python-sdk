from firebolt.firebolt_client import FireboltClient


def test_todo():
    assert FireboltClient is not None
    # with FireboltClient.from_env() as fc:
    #     print(fc.account_id)
