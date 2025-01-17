# firebolt-sdk
[![Nightly code check](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/nightly.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/nightly.yml)
[![Unit tests](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/unit-tests.yml)
[![Code quality checks](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/code-check.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/code-check.yml)
[![Firebolt Security Scan](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/security-scan.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/security-scan.yml)
[![Integration tests](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/integration-tests.yml/badge.svg)](https://github.com/firebolt-db/firebolt-python-sdk/actions/workflows/integration-tests.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/ptiurin/65d5a42849fd78f4c6e62fad18490d20/raw/firebolt-sdk-coverage.json)


### Installation

* Requires Python `>=3.8`
* `pip install "firebolt-sdk>=1.0.0a1"`

## Documentation

For reference and tutorials, see the [Firebolt Python SDK reference](https://python.docs.firebolt.io/sdk_documenation/latest/).

## Connection parameters
These parameters are used to connect to a Firebolt database:
- **account_name** - name of firebolt account
- **client_id** - credentials client id
- **cliend_secret** - credentials client secret
- **database [Optional]** - name of the database to connect to
- **engine_name [Optional]** - name of the engine to connect to

## Examples
See [PEP-249](https://www.python.org/dev/peps/pep-0249) for the DB API reference and specifications. An example [jupyter notebook](https://github.com/firebolt-db/firebolt-sdk/tree/main/examples/dbapi.ipynb) is included to illustrate the use of the Firebolt API.

## Special considerations
### Cursor objects should not be shared between threads
Cursor is not thread-safe and should not be shared across threads. In a multi-threaded environment you can share a Connection, but each thread would need to keep its own Cursor. This corresponds to a thread safety 2 in the [DBApi specification](https://peps.python.org/pep-0249/#threadsafety).

### Some keywords are not allowed as SET parameters

```Python
cursor.execute("SET parameter=value")
```
This will error out if you try to set `account_id`, `output_format`, `database`, `engine`. These are special keywords that should not be set directly. To switch between databases and engines use `USE DATABASE/ENGINE`.

## Optional features
### Faster datetime with ciso8601
By default, firebolt-sdk uses `datetime` module to parse date and datetime values, which might be slow for a large amount of operations. In order to speed up datetime operations, it's possible to use [ciso8601](https://pypi.org/project/ciso8601/) package. In order to install firebolt-sdk with `ciso8601` support, run `pip install "firebolt-sdk[ciso8601]"`

## Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD)

## License
The Firebolt DB API is licensed under the [Apache License Version 2.0](https://github.com/firebolt-db/firebolt-sdk/tree/main/LICENSE) software license.
