from firebolt.firebolt_client import FireboltClient, firebolt_client


class Database:
    def __init__(self, firebolt_client: FireboltClient = firebolt_client):
        self.firebolt_client = firebolt_client  # could be replaced with DI framework
        self.http_client = self.firebolt_client.http_client

    def get_id_by_name(self, database_name: str) -> str:
        response = self.http_client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def get_by_id(self, database_id: str):
        response = self.http_client.get(
            url=f"/core/v1/accounts/{self.firebolt_client.account_id}/databases/{database_id}",
        )
        spec = response.json()["database"]
        return spec


db = Database()
id = db.get_id_by_name("eg_sandbox")
print(id)
print(db.get_by_id(id))
