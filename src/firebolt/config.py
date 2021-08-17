# deprecate?
import sys

print(sys.path)

from firebolt.http_client import get_http_client

CONFIG = {
    "prod": {"host": "api.app.firebolt.io"},
    "staging": {"host": "api.staging.firebolt.io"},
    "dev": {"host": "api.dev.firebolt.io"},
}

print(get_http_client)
