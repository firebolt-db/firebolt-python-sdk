# firebolt-sdk
### Installation

* Requires Python `>=3.7`
* `pip install firebolt-sdk`


## Connection parameters
These parameters are used to connect to a Firebolt database:
- **engine_url** - url for an engine to make requests to. Can be retrieved from Web UI, or from [engine](https://github.com/firebolt-db/firebolt-sdk/tree/main/src/firebolt/model/engine.py) attribute `endpoint`
- **database** - name of the database to make queries to
- **username** - account username
- **password** - account password

Optional parameters
- **api_endpoint** - API hostname for logging in. Defaults to `api.app.firebolt.io`.
- **account_name** - For users with more than one account. If not specified uses default account.

## Examples
See [PEP-249](https://www.python.org/dev/peps/pep-0249) for the DB API reference and specifications. An example [jupyter notebook](https://github.com/firebolt-db/firebolt-sdk/tree/main/examples/dbapi.ipynb) is included to illustrate the use of the Firebolt API.

### Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD)

## License
The Firebolt DB API is licensed under the [Apache License Version 2.0](https://github.com/firebolt-db/firebolt-sdk/tree/main/LICENSE) software license.
