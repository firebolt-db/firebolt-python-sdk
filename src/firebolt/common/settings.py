from pydantic import BaseSettings, Field, SecretStr, root_validator


class Settings(BaseSettings):
    # Authorization
    user: str = Field(None, env="FIREBOLT_USER")
    password: SecretStr = Field(None, env="FIREBOLT_PASSWORD")
    # Or
    access_token: str = Field(None, env="FIREBOLT_AUTH_TOKEN")

    account_name: str = Field(None, env="FIREBOLT_ACCOUNT")
    server: str = Field(..., env="FIREBOLT_SERVER")
    default_region: str = Field(..., env="FIREBOLT_DEFAULT_REGION")

    class Config:
        env_file = ".env"

    @root_validator
    def mutual_exclusive_with_creds(cls, values: dict) -> dict:
        if values["user"] or values["password"]:
            if values["access_token"]:
                raise ValueError("Provide only one of user/password or access_token")
        elif not values["access_token"]:
            raise ValueError("Provide either user/password or access_token")
        return values
