AUTH_SERVICE_ACCOUNT_URL = "/oauth/token"

DATABASES_URL = "/core/v1/account/databases"

ENGINES_URL = "/core/v1/account/engines"
ENGINES_BY_IDS_URL = "/core/v1/engines:getByIds"

ACCOUNT_BY_NAME_URL = "/web/v3/account/{account_name}/resolve"

ACCOUNT_DATABASES_URL = "/core/v1/accounts/{account_id}/databases"
ACCOUNT_DATABASE_URL = "/core/v1/accounts/{account_id}/databases/{database_id}"
ACCOUNT_DATABASE_BINDING_URL = ACCOUNT_DATABASE_URL + "/bindings/{engine_id}"
ACCOUNT_DATABASE_BY_NAME_URL = ACCOUNT_DATABASES_URL + ":getIdByName"

ACCOUNT_BINDINGS_URL = "/core/v1/accounts/{account_id}/bindings"

ACCOUNT_INSTANCE_TYPES_URL = "/aws/v2/accounts/{account_id}/instanceTypes"

PROVIDERS_URL = "/compute/v1/providers"
REGIONS_URL = "/compute/v1/regions"

GATEWAY_HOST_BY_ACCOUNT_NAME = "/web/v3/account/{account_name}/engineUrl"

# V1 URLS

ACCOUNT_ENGINE_URL = "/core/v1/accounts/{account_id}/engines/{engine_id}"
ACCOUNT_ENGINE_START_URL = ACCOUNT_ENGINE_URL + ":start"
ACCOUNT_ENGINE_RESTART_URL = ACCOUNT_ENGINE_URL + ":restart"
ACCOUNT_ENGINE_STOP_URL = ACCOUNT_ENGINE_URL + ":stop"
ACCOUNT_LIST_ENGINES_URL = "/core/v1/accounts/{account_id}/engines"
ACCOUNT_ENGINE_ID_BY_NAME_URL = ACCOUNT_LIST_ENGINES_URL + ":getIdByName"
ACCOUNT_ENGINE_REVISION_URL = ACCOUNT_ENGINE_URL + "/engineRevisions/{revision_id}"

AUTH_URL = "/auth/v1/login"
ACCOUNT_ENGINE_URL_BY_DATABASE_NAME_V1 = (
    ACCOUNT_LIST_ENGINES_URL + ":getURLByDatabaseName"
)
ACCOUNT_LIST_ENGINES_URL = "/core/v1/accounts/{account_id}/engines"
ACCOUNT_ENGINE_ID_BY_NAME_URL = ACCOUNT_LIST_ENGINES_URL + ":getIdByName"
ACCOUNT_URL = "/iam/v2/account"
ACCOUNT_BY_NAME_URL_V1 = "/iam/v2/accounts:getIdByName"
