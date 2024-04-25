import json
from re import Pattern, compile
from typing import Callable, List
from urllib.parse import urlparse

import httpx
from httpx import Request
from pytest import fixture

from firebolt.client.auth.base import Auth
from firebolt.model.V1.binding import Binding, BindingKey
from firebolt.model.V1.database import Database, DatabaseKey
from firebolt.model.V1.engine import Engine, EngineKey, EngineSettings
from firebolt.model.V1.provider import Provider
from firebolt.model.V1.region import Region, RegionKey
from firebolt.utils.exception import AccountNotFoundError
from firebolt.utils.urls import (
    ACCOUNT_BINDINGS_URL,
    ACCOUNT_BY_NAME_URL,
    ACCOUNT_DATABASE_BINDING_URL,
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_DATABASE_URL,
    ACCOUNT_DATABASES_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_INSTANCE_TYPES_URL,
    ACCOUNT_LIST_ENGINES_URL,
    ACCOUNT_URL,
    AUTH_URL,
    ENGINES_BY_IDS_URL,
    PROVIDERS_URL,
    REGIONS_URL,
)
from tests.unit.response import Response
from tests.unit.util import (
    list_to_paginated_response_v1 as list_to_paginated_response,
)


@fixture
def engine_name() -> str:
    return "my_engine"


@fixture
def engine_scale() -> int:
    return 2


@fixture
def engine_settings() -> EngineSettings:
    return EngineSettings.default()


@fixture
def region_key() -> RegionKey:
    return RegionKey(provider_id="pid", region_id="rid")


@fixture
def db_id() -> str:
    return "db_id"


@fixture
def mock_engine(engine_name, region_key, engine_settings, account_id, server) -> Engine:
    return Engine(
        name=engine_name,
        compute_region_id=region_key,
        settings=engine_settings,
        key=EngineKey(account_id=account_id, engine_id="mock_engine_id_1"),
        endpoint=f"https://{server}",
    )


@fixture
def provider() -> Provider:
    return Provider(
        provider_id="mock_provider_id",
        name="mock_provider_name",
    )


@fixture
def mock_providers(provider) -> List[Provider]:
    return [provider]


@fixture
def provider_callback(provider_url: str, mock_providers) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == provider_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_providers),
        )

    return do_mock


@fixture
def provider_url(server: str) -> str:
    return f"https://{server}{PROVIDERS_URL}"


@fixture
def region_1(provider) -> Region:
    return Region(
        key=RegionKey(
            provider_id=provider.provider_id,
            region_id="mock_region_id_1",
        ),
        name="mock_region_1",
    )


@fixture
def region_2(provider) -> Region:
    return Region(
        key=RegionKey(
            provider_id=provider.provider_id,
            region_id="mock_region_id_2",
        ),
        name="mock_region_2",
    )


@fixture
def mock_regions(region_1, region_2) -> List[Region]:
    return [region_1, region_2]


@fixture
def region_callback(region_url: str, mock_regions) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == region_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_regions),
        )

    return do_mock


@fixture
def region_url(server: str) -> str:
    return f"https://{server}{REGIONS_URL}?page.first=5000"


@fixture
def instance_type_callback(instance_type_url: str, mock_instance_types) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == instance_type_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_instance_types),
        )

    return do_mock


@fixture
def instance_type_region_1_callback(
    instance_type_region_1_url: str, mock_instance_types
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == instance_type_region_1_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_instance_types),
        )

    return do_mock


@fixture
def instance_type_empty_callback() -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([]),
        )

    return do_mock


@fixture
def instance_type_url(server: str, account_id: str) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_INSTANCE_TYPES_URL.format(account_id=account_id)
        + "?page.first=5000"
    )


@fixture
def instance_type_region_1_url(server: str, region_1: Region, account_id: str) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_INSTANCE_TYPES_URL.format(account_id=account_id)
        + f"?page.first=5000&filter.id_region_id_eq={region_1.key.region_id}"
    )


@fixture
def instance_type_region_2_url(server: str, region_2: Region, account_id: str) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_INSTANCE_TYPES_URL.format(account_id=account_id)
        + f"?page.first=5000&filter.id_region_id_eq={region_2.key.region_id}"
    )


@fixture
def engine_callback(engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert urlparse(engine_url).path in request.url.path
        return Response(
            status_code=httpx.codes.OK,
            json={"engine": mock_engine.model_dict()},
        )

    return do_mock


@fixture
def engine_url(server: str, account_id) -> str:
    return f"https://{server}" + ACCOUNT_LIST_ENGINES_URL.format(account_id=account_id)


@fixture
def account_engine_callback(account_engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == account_engine_url
        return Response(
            status_code=httpx.codes.OK,
            json={"engine": mock_engine.model_dict()},
        )

    return do_mock


@fixture
def many_engines_callback(mock_engine: Engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json={"edges": [{"node": mock_engine.model_dict()}]},
        )

    return do_mock


@fixture
def account_engine_url(server: str, account_id, mock_engine) -> str:
    return f"https://{server}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id,
        engine_id=mock_engine.engine_id,
    )


@fixture
def db_description() -> str:
    return "database description"


@fixture
def mock_database(region_1: Region, account_id: str, database_id: str) -> Database:
    return Database(
        name="mock_db_name",
        description="mock_db_description",
        compute_region_key=region_1.key,
        database_key=DatabaseKey(account_id=account_id, database_id=database_id),
    )


@fixture
def create_databases_callback(databases_url: str, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        database_properties = json.loads(request.read().decode("utf-8"))["database"]

        mock_database.name = database_properties["name"]
        mock_database.description = database_properties["description"]

        assert request.url == databases_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.model_dict()},
        )

    return do_mock


@fixture
def databases_get_callback(databases_url: str, mock_database) -> Callable:
    def get_databases_callback_inner(
        request: httpx.Request = None, **kwargs
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK,
            json={"edges": [{"node": mock_database.model_dict()}]},
        )

    return get_databases_callback_inner


@fixture
def databases_url(server: str, account_id: str) -> str:
    return f"https://{server}" + ACCOUNT_DATABASES_URL.format(account_id=account_id)


@fixture
def database_callback(database_url: str, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.model_dict()},
        )

    return do_mock


@fixture
def database_not_found_callback(database_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_url
        return Response(
            status_code=httpx.codes.OK,
            json={},
        )

    return do_mock


@fixture
def database_url(server: str, account_id: str, database_id: str) -> str:
    return f"https://{server}" + ACCOUNT_DATABASE_URL.format(
        account_id=account_id, database_id=database_id
    )


@fixture
def database_get_by_name_callback(database_get_by_name_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_by_name_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database_id": {"database_id": mock_database.database_id}},
        )

    return do_mock


@fixture
def database_get_by_name_url(server: str, account_id: str, mock_database) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=account_id)
        + f"?database_name={mock_database.name}"
    )


@fixture
def database_update_callback(database_get_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        database_properties = json.loads(request.read().decode("utf-8"))["database"]

        assert request.url == database_get_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": database_properties},
        )

    return do_mock


@fixture
def database_get_callback(database_get_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_url
        return Response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.model_dict()},
        )

    return do_mock


# duplicates database_url
@fixture
def database_get_url(server: str, account_id: str, mock_database) -> str:
    return f"https://{server}" + ACCOUNT_DATABASE_URL.format(
        account_id=account_id, database_id=mock_database.database_id
    )


@fixture
def no_bindings_callback(bindings_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == bindings_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([]),
        )

    return do_mock


@fixture
def bindings_url(server: str, account_id: str, mock_engine: Engine) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_BINDINGS_URL.format(account_id=account_id)
        + f"?page.first=5000&filter.id_engine_id_eq={mock_engine.engine_id}"
    )


@fixture
def database_bindings_url(server: str, account_id: str, mock_database: Database) -> str:
    return (
        f"https://{server}"
        + ACCOUNT_BINDINGS_URL.format(account_id=account_id)
        + f"?page.first=5000&filter.id_database_id_eq={mock_database.database_id}"
    )


@fixture
def create_binding_url(
    server: str, account_id: str, mock_database: Database, mock_engine: Engine
) -> str:
    return f"https://{server}" + ACCOUNT_DATABASE_BINDING_URL.format(
        account_id=account_id,
        database_id=mock_database.database_id,
        engine_id=mock_engine.engine_id,
    )


@fixture
def create_binding_callback(create_binding_url: str, binding) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == create_binding_url
        return Response(
            status_code=httpx.codes.OK,
            json={"binding": binding.model_dict()},
        )

    return do_mock


@fixture
def binding(account_id, mock_engine, database_id) -> Binding:
    return Binding(
        binding_key=BindingKey(
            account_id=account_id,
            database_id=database_id,
            engine_id=mock_engine.engine_id,
        ),
        is_default_engine=True,
    )


@fixture
def bindings_callback(bindings_url: str, binding: Binding) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == bindings_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([binding]),
        )

    return do_mock


@fixture
def bindings_database_callback(
    database_bindings_url: str, binding: Binding
) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_bindings_url
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([binding]),
        )

    return do_mock


@fixture
def auth_url(server: str) -> str:
    return f"https://{server}{AUTH_URL}"


@fixture
def auth_callback(auth_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == auth_url
        return Response(
            status_code=httpx.codes.OK,
            json={"access_token": "", "expires_in": 2**32},
        )

    return do_mock


@fixture
def check_credentials_callback(user: str, password: str, access_token: str) -> Callable:
    def check_credentials(
        request: Request = None,
        **kwargs,
    ) -> Response:
        assert request, "empty request"
        body = json.loads(request.read())
        assert "username" in body, "Missing username"
        assert body["username"] == user, "Invalid username"
        assert "password" in body, "Missing password"
        assert body["password"] == password, "Invalid password"

        return Response(
            status_code=httpx.codes.OK,
            json={"expires_in": 2**32, "access_token": access_token},
        )

    return check_credentials


@fixture
def account_id_url(server: str) -> Pattern:
    base = f"https://{server}{ACCOUNT_BY_NAME_URL}?account_name="
    default_base = f"https://{server}{ACCOUNT_URL}"
    base = base.replace("/", "\\/").replace("?", "\\?")
    default_base = default_base.replace("/", "\\/").replace("?", "\\?")
    return compile(f"(?:{base}.*|{default_base})")


@fixture
def account_id_callback(
    account_id: str,
    account_name: str,
) -> Callable:
    def do_mock(
        request: Request,
        **kwargs,
    ) -> Response:
        if "account_name" not in request.url.params:
            return Response(
                status_code=httpx.codes.OK, json={"account": {"id": account_id}}
            )
        # In this case, an account_name *should* be specified.
        if request.url.params["account_name"] != account_name:
            raise AccountNotFoundError(request.url.params["account_name"])
        return Response(status_code=httpx.codes.OK, json={"account_id": account_id})

    return do_mock


@fixture
def auth(username_password_auth) -> Auth:
    return username_password_auth


@fixture
def engines_by_id_url(server: str) -> str:
    return f"https://{server}" + ENGINES_BY_IDS_URL
