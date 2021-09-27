from firebolt.client import FireboltClient
from firebolt.model import FireboltBaseModel
from firebolt.model.database import Database
from firebolt.service.base_service import BaseService
from firebolt.service.region_service import RegionService


class DatabaseService(BaseService):
    def __init__(self, firebolt_client: FireboltClient):
        self.region_service = RegionService(firebolt_client=firebolt_client)
        super().__init__(firebolt_client=firebolt_client)

    def get_by_id(self, database_id: str) -> Database:
        """Get a Database from Firebolt by its id."""
        response = self.firebolt_client.get(
            url=f"/core/v1/accounts/{self.account_id}/databases/{database_id}",
        )
        database_spec: dict = response.json()["database"]
        return Database.parse_obj(database_spec)

    def get_by_name(self, database_name: str) -> Database:
        """Get a Database from Firebolt by its name."""
        database_id = self.get_id_by_name(database_name=database_name)
        return self.get_by_id(database_id=database_id)

    def get_id_by_name(self, database_name: str) -> str:
        """Get a Database id from Firebolt by its name."""
        response = self.firebolt_client.get(
            url=f"/core/v1/account/databases:getIdByName",
            params={"database_name": database_name},
        )
        database_id = response.json()["database_id"]["database_id"]
        return database_id

    def create(self, database_name: str, region_name: str) -> Database:
        """
        Create a new Database on Firebolt.

        Args:
            database_name: Name of the database.
            region_name: Region name in which to create the database.

        Returns:
            The newly created Database.
        """

        class _DatabaseCreateRequest(FireboltBaseModel):
            """Helper model for sending Database creation requests."""

            account_id: str
            database: Database

        region_key = self.region_service.get_by_name(region_name=region_name).key
        database = Database(name=database_name, compute_region_key=region_key)

        response = self.firebolt_client.post(
            url=f"/core/v1/accounts/{self.account_id}/databases",
            headers={"Content-type": "application/json"},
            json=_DatabaseCreateRequest(
                account_id=self.account_id,
                database=database,
            ).dict(by_alias=True),
        )
        return Database.parse_obj(response.json()["database"])

    def delete(self, database_id: str) -> Database:
        """Delete a database from Firebolt."""
        response = self.firebolt_client.delete(
            url=f"/core/v1/account/databases/{database_id}",
            headers={"Content-type": "application/json"},
        )
        return Database.parse_obj(response.json()["database"])
