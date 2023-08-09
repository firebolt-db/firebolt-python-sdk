# firebolt-sdk
[![Nightly code check](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/nightly.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/nightly.yml)
[![Unit tests](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/unit-tests.yml)
[![Code quality checks](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/code-check.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/code-check.yml)
[![Firebolt Security Scan](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/security-scan.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/security-scan.yml)
[![Integration tests](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/integration-tests.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ptiurin/65d5a42849fd78f4c6e62fad18490d20/raw/firebolt-sdk-coverage.json)


### Installation

* Requires Python `>=3.7`
* `pip install firebolt-sdk`

## Documentation

For reference and tutorials, see the [Firebolt Python SDK reference](https://python-sdk.docs.firebolt.io/en/0.x/).

## Connection parameters
These parameters are used to connect to a Firebolt database:
- Username/Password authentication
  - **username** - account username
  - **password** - account password
- Service Account authentication
  - **client_id** - service account client id
  - **client_secret** - service account client secret
- **database** - name of the database to make queries to
- **engine_url** - url for an engine to make requests to. Can be retrieved from Web UI, or from [engine](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/src/firebolt/model/engine.py) attribute `endpoint`   
  (Mutually exclusive with **engine_name**)
- **engine_name** - name of an engine to make requests to. Can be retrieved from Web UI, or from [engine](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/src/firebolt/model/engine.py) attribute `name`   
  (Mutually exclusive with **engine_url**)

Optional parameters
- **account_name** - name of a Firebolt account to use. Default account is used if it's not provided
- **use_token_cache** - whether to store obtained authentication tokens in cache file. Defaults to True
- **api_endpoint** - api hostname for logging in. Defaults to `api.app.firebolt.io`.

## Examples
See [PEP-249](https://www.python.org/dev/peps/pep-0249) for the DB API reference and specifications. An example [jupyter notebook](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/examples/dbapi.ipynb) is included to illustrate the use of the Firebolt API.

## Special considerations
### Cursor objects should not be shared between threads
Cursor is not thread-safe and should not be shared across threads. In a multi-threaded environment you can share a Connection, but each thread would need to keep its own Cursor. This corresponds to a thread safety 2 in the [DBApi specification](https://peps.python.org/pep-0249/#threadsafety).

## Optional features
### Faster datetime with ciso8601
By default, firebolt-sdk uses `datetime` module to parse date and datetime values, which might be slow for a large amount of operations. In order to speed up datetime operations, it's possible to use [ciso8601](https://pypi.org/project/ciso8601/) package. In order to install firebolt-sdk with `ciso8601` support, run `pip install "firebolt-sdk[ciso8601]"`

## Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/CONTRIBUTING.MD)

## License
The Firebolt DB API is licensed under the [Apache License Version 2.0](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/LICENSE) software license.
