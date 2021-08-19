from firebolt.firebolt_client import FireboltClient


def test_todo():
    assert FireboltClient is not None
    # with FireboltClient.from_env() as f:
    #     print(f.account_id)
    #     print(f.databases.get_by_name("eg_sandbox"))
