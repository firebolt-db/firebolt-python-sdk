from pydantic import BaseSettings, Field, SecretStr


class Settings(BaseSettings):
    server: str = Field(..., env="FIREBOLT_SERVER")
    user: str = Field(..., env="FIREBOLT_USER")
    password: SecretStr = Field(..., env="FIREBOLT_PASSWORD")
    default_region: str = Field(..., env="FIREBOLT_DEFAULT_REGION")

    class Config:
        env_file = ".env"
