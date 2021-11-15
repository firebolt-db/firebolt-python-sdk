from typing import Callable, List

import httpx
import pytest
from pytest_httpx import to_response
from pytest_httpx._httpx_internals import Response

from firebolt.common.settings import Settings
from firebolt.common.urls import (
    ACCOUNT_BINDINGS_URL,
    ACCOUNT_DATABASE_BINDING_URL,
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_DATABASE_URL,
    ACCOUNT_DATABASES_URL,
    ACCOUNT_ENGINES_URL,
    INSTANCE_TYPES_URL,
    PROVIDERS_URL,
    REGIONS_URL,
)
from firebolt.model.binding import Binding, BindingKey
from firebolt.model.database import Database, DatabaseKey
from firebolt.model.engine import Engine, EngineKey, EngineSettings
from firebolt.model.instance_type import InstanceType, InstanceTypeKey
from tests.unit.util import list_to_paginated_response


@pytest.fixture
def engine_name() -> str:
    return "my_engine"


@pytest.fixture
def engine_settings() -> EngineSettings:
    return EngineSettings.default()


@pytest.fixture
def mock_engine(engine_name, region_1, engine_settings, account_id, settings) -> Engine:
    return Engine(
        name=engine_name,
        compute_region_key=region_1.key,
        settings=engine_settings,
        key=EngineKey(account_id=account_id, engine_id="mock_engine_id_1"),
        endpoint=f"https://{settings.server}",
    )


@pytest.fixture
def instance_type_1(provider, region_1) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_1.key.region_id,
            instance_type_id="instance_type_id_1",
        ),
        name="i3.4xlarge",
    )


@pytest.fixture
def instance_type_2(provider, region_2) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_2.key.region_id,
            instance_type_id="instance_type_id_2",
        ),
        name="i3.8xlarge",
    )


@pytest.fixture
def mock_instance_types(instance_type_1, instance_type_2) -> List[InstanceType]:
    return [instance_type_1, instance_type_2]


@pytest.fixture
def provider_callback(provider_url: str, mock_providers) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == provider_url
        return to_response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_providers),
        )

    return do_mock


@pytest.fixture
def provider_url(settings: Settings) -> str:
    return f"https://{settings.server}{PROVIDERS_URL}"


@pytest.fixture
def region_callback(region_url: str, mock_regions) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == region_url
        return to_response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_regions),
        )

    return do_mock


@pytest.fixture
def region_url(settings: Settings) -> str:
    return f"https://{settings.server}{REGIONS_URL}?page.first=5000"


@pytest.fixture
def instance_type_callback(instance_type_url: str, mock_instance_types) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == instance_type_url
        return to_response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_instance_types),
        )

    return do_mock


@pytest.fixture
def instance_type_url(settings: Settings) -> str:
    return f"https://{settings.server}{INSTANCE_TYPES_URL}?page.first=5000"


@pytest.fixture
def engine_callback(engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == engine_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"engine": mock_engine.dict()},
        )

    return do_mock


@pytest.fixture
def engine_url(settings: Settings, account_id) -> str:
    return f"https://{settings.server}" + ACCOUNT_ENGINES_URL.format(
        account_id=account_id
    )


@pytest.fixture
def mock_database(db_name, region_1, account_id) -> Database:
    return Database(
        name=db_name,
        compute_region_key=region_1.key,
        database_key=DatabaseKey(
            account_id=account_id, database_id="mock_database_id_1"
        ),
    )


@pytest.fixture
def databases_callback(databases_url: str, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == databases_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.dict()},
        )

    return do_mock


@pytest.fixture
def databases_url(settings: Settings, account_id: str) -> str:
    return f"https://{settings.server}" + ACCOUNT_DATABASES_URL.format(
        account_id=account_id
    )


@pytest.fixture
def database_callback(database_url: str, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.dict()},
        )

    return do_mock


@pytest.fixture
def database_not_found_callback(database_url: str) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_url
        return to_response(
            status_code=httpx.codes.OK,
            json={},
        )

    return do_mock


@pytest.fixture
def database_url(settings: Settings, account_id: str, mock_database) -> str:
    return f"https://{settings.server}" + ACCOUNT_DATABASE_URL.format(
        account_id=account_id, database_id=mock_database.database_id
    )


@pytest.fixture
def database_get_by_name_callback(database_get_by_name_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_by_name_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"database_id": {"database_id": mock_database.database_id}},
        )

    return do_mock


@pytest.fixture
def database_get_by_name_url(settings: Settings, account_id: str, mock_database) -> str:
    return (
        f"https://{settings.server}"
        + ACCOUNT_DATABASE_BY_NAME_URL.format(account_id=account_id)
        + f"?database_name={mock_database.name}"
    )


@pytest.fixture
def database_get_callback(database_get_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"database": mock_database.dict()},
        )

    return do_mock


# duplicates database_url
@pytest.fixture
def database_get_url(settings: Settings, account_id: str, mock_database) -> str:
    return f"https://{settings.server}" + ACCOUNT_DATABASE_URL.format(
        account_id=account_id, database_id=mock_database.database_id
    )


@pytest.fixture
def binding(account_id, mock_engine, mock_database) -> Binding:
    return Binding(
        binding_key=BindingKey(
            account_id=account_id,
            database_id=mock_database.database_id,
            engine_id=mock_engine.engine_id,
        ),
        is_default_engine=True,
    )


@pytest.fixture
def bindings_callback(bindings_url: str, binding: Binding) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == bindings_url
        return to_response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([binding]),
        )

    return do_mock


@pytest.fixture
def bindings_url(settings: Settings, account_id: str, mock_engine: Engine) -> str:
    return (
        f"https://{settings.server}"
        + ACCOUNT_BINDINGS_URL.format(account_id=account_id)
        + f"?page.first=5000&filter.id_engine_id_eq={mock_engine.engine_id}"
    )


@pytest.fixture
def create_binding_callback(create_binding_url: str, binding) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == create_binding_url
        return to_response(
            status_code=httpx.codes.OK,
            json={"binding": binding.dict()},
        )

    return do_mock


@pytest.fixture
def create_binding_url(
    settings: Settings, account_id: str, mock_database: Database, mock_engine: Engine
) -> str:
    return f"https://{settings.server}" + ACCOUNT_DATABASE_BINDING_URL.format(
        account_id=account_id,
        database_id=mock_database.database_id,
        engine_id=mock_engine.engine_id,
    )
