from os import environ

from firebolt.common.settings import Settings
from firebolt.service.manager import ResourceManager

if __name__ == "__main__":
    rm = ResourceManager(Settings())

    if "DATABASE" not in environ:
        raise RuntimeError("DATABASE environment variable should be defined")
    database_name = environ.get("DATABASE")
    rm.databases.create(database_name)
