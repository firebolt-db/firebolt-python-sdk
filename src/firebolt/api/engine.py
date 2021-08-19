class EngineService:
    def __init__(self, firebolt_client):
        self.firebolt_client = firebolt_client
        self.http_client = firebolt_client.http_client

    def get_engine_id_by_name(self, engine_name: str) -> str:
        response = self.http_client.get(
            url=f"core/v1/account/engines:getIdByName",
            params={"engine_name": engine_name},
        )
        engine_id = response.json()["engine_id"]["engine_id"]
        return engine_id

    def start_engine(self, engine_id: str) -> str:
        response = self.http_client.get(
            url=f"core/v1/account/engines/{engine_id}:start",
        )
        status = response.json()["engine"]["current_status_summary"]
        return status
