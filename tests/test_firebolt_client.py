from firebolt.firebolt_client import FireboltClient


def test_todo():
    assert FireboltClient is not None
    with FireboltClient.from_env() as fc:
        assert fc
        # j = fc.engines.get_engine_by_name(engine_name="eg_sandbox_ingest")
        # print(j)
        # '569ea275-3f21-4ac4-9750-b757cfba5bd6'

        # with pathlib.Path("./engine.json").open("w") as f:
        #     f.write(json.dumps(j))

        # print(fc.account_id)

        # db = fc.databases.get_by_name("eg_sandbox")
        # print(type(db))
        # print(db)

        # engine = fc.engines.get_by_name("eg_sandbox_ingest")
        # print(type(engine))
        # print(engine)

        # print(db_json)
        # db = Database.parse_obj(db_json)
        # print(db)
        # debug(engine.json())
        # debug(engine)
