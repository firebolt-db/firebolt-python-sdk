import json
from typing import Callable, List
from urllib.parse import urlparse

import httpx
import pytest
from httpx import Response

from firebolt.common.settings import Settings
from firebolt.model.binding import Binding, BindingKey
from firebolt.model.database import Database, DatabaseKey
from firebolt.model.engine import Engine, EngineKey, EngineSettings
from firebolt.model.engine_revision import (
    EngineRevision,
    EngineRevisionSpecification,
)
from firebolt.model.instance_type import InstanceType, InstanceTypeKey
from firebolt.model.region import Region
from firebolt.utils.urls import (
    ACCOUNT_BINDINGS_URL,
    ACCOUNT_DATABASE_BINDING_URL,
    ACCOUNT_DATABASE_BY_NAME_URL,
    ACCOUNT_DATABASE_URL,
    ACCOUNT_DATABASES_URL,
    ACCOUNT_ENGINE_URL,
    ACCOUNT_ENGINES_URL,
    INSTANCE_TYPES_URL,
    PROVIDERS_URL,
    REGIONS_URL,
)
from tests.unit.util import list_to_paginated_response


@pytest.fixture
def engine_name() -> str:
    return "my_engine"


@pytest.fixture
def engine_scale() -> int:
    return 2


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
def mock_engine_revision_spec(
    instance_type_2, engine_scale
) -> EngineRevisionSpecification:
    return EngineRevisionSpecification(
        db_compute_instances_type_key=instance_type_2.key,
        db_compute_instances_count=engine_scale,
        proxy_instances_type_key=instance_type_2.key,
    )


@pytest.fixture
def mock_engine_revision(mock_engine_revision_spec) -> EngineRevision:
    return EngineRevision(specification=mock_engine_revision_spec)


@pytest.fixture
def instance_type_1(provider, region_1) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_1.key.region_id,
            instance_type_id="instance_type_id_1",
        ),
        name="B1",
        price_per_hour_cents=10,
        storage_size_bytes=0,
    )


@pytest.fixture
def instance_type_2(provider, region_2) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_2.key.region_id,
            instance_type_id="instance_type_id_2",
        ),
        name="B2",
        price_per_hour_cents=20,
        storage_size_bytes=500,
    )


@pytest.fixture
def instance_type_3(provider, region_2) -> InstanceType:
    return InstanceType(
        key=InstanceTypeKey(
            provider_id=provider.provider_id,
            region_id=region_2.key.region_id,
            instance_type_id="instance_type_id_2",
        ),
        name="B2",
        price_per_hour_cents=30,
        storage_size_bytes=500,
    )


@pytest.fixture
def cheapest_instance(instance_type_2) -> InstanceType:
    return instance_type_2


@pytest.fixture
def mock_instance_types(
    instance_type_1, instance_type_2, instance_type_3
) -> List[InstanceType]:
    return [instance_type_1, instance_type_2, instance_type_3]


@pytest.fixture
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
        return Response(
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
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response(mock_instance_types),
        )

    return do_mock


@pytest.fixture
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


@pytest.fixture
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


@pytest.fixture
def instance_type_url(settings: Settings) -> str:
    return f"https://{settings.server}{INSTANCE_TYPES_URL}?page.first=5000"


@pytest.fixture
def instance_type_region_1_url(settings: Settings, region_1: Region) -> str:
    return (
        f"https://{settings.server}{INSTANCE_TYPES_URL}?page.first=5000&"
        f"filter.id_region_id_eq={region_1.key.region_id}"
    )


@pytest.fixture
def instance_type_region_2_url(settings: Settings, region_2: Region) -> str:
    return (
        f"https://{settings.server}{INSTANCE_TYPES_URL}?page.first=5000&"
        f"filter.id_region_id_eq={region_2.key.region_id}"
    )


@pytest.fixture
def engine_callback(engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert urlparse(engine_url).path in request.url.path
        return Response(
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
def account_engine_callback(account_engine_url: str, mock_engine) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == account_engine_url
        return Response(
            status_code=httpx.codes.OK,
            json={"engine": mock_engine.dict()},
        )

    return do_mock


@pytest.fixture
def account_engine_url(settings: Settings, account_id, mock_engine) -> str:
    return f"https://{settings.server}" + ACCOUNT_ENGINE_URL.format(
        account_id=account_id,
        engine_id=mock_engine.engine_id,
    )


@pytest.fixture
def mock_database(region_1, account_id) -> Database:
    return Database(
        name="mock_db_name",
        description="mock_db_description",
        compute_region_key=region_1.key,
        database_key=DatabaseKey(
            account_id=account_id, database_id="mock_database_id_1"
        ),
    )


@pytest.fixture
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
            json={"database": mock_database.dict()},
        )

    return do_mock


@pytest.fixture
def databases_get_callback(databases_url: str, mock_database) -> Callable:
    def get_databases_callback_inner(
        request: httpx.Request = None, **kwargs
    ) -> Response:
        return Response(
            status_code=httpx.codes.OK, json={"edges": [{"node": mock_database.dict()}]}
        )

    return get_databases_callback_inner


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
        return Response(
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
        return Response(
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
        return Response(
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


@pytest.fixture
def database_get_callback(database_get_url, mock_database) -> Callable:
    def do_mock(
        request: httpx.Request = None,
        **kwargs,
    ) -> Response:
        assert request.url == database_get_url
        return Response(
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
        return Response(
            status_code=httpx.codes.OK,
            json=list_to_paginated_response([binding]),
        )

    return do_mock


@pytest.fixture
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
        return Response(
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
