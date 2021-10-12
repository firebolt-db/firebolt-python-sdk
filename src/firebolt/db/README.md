# Firebolt DB API
The Firebolt DB API is a Python 3 implementation of PEP-249 for Firebolt.

## Connection parameters
These parameters are used to connect to a Firebolt database:
- **engine_url** - url for an engine to make requests to. Can be retrieved from Web UI, or from [engine](../models/engine.py#L57) attribute `endpoint`
- **database** - name of the database to make queries to
- **username** - account username
- **password** - account password

Optional parameters
- **api_endpoint** - api hostname for logging in. Defaults to `api.app.firebolt.io`.

## Examples
See [PEP-249](https://www.python.org/dev/peps/pep-0249) for the DB API reference and specifications. An example [jupyter notebook](examples.ipynb) is included to illustrate the use of the Firebolt API.

## License
The Firebolt DB API is licensed under the [Apache License Version 2.0](https://github.com/hyperledger/fabric-sdk-py/blob/main/LICENSE) software license.

<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a>
