# firebolt-sdk

### Installation & Usage

* Requires Python `>=3.9`
* Clone this repo
* From the cloned directory: `pip install .`
* See [examples.ipynb](examples.ipynb) for usage

### Configuration

To use the client, you generally will want to set the following environment variables:
```
FIREBOLT_USER='email@domain.com'
FIREBOLT_PASSWORD='*****'
FIREBOLT_SERVER='api.app.firebolt.io'
FIREBOLT_DEFAULT_REGION='us-east-1'
FIREBOLT_DEFAULT_PROVIDER='AWS'
```

* The provider is optional; if you do not provide it, we assume 'AWS'
* You can store these in a `.env` file 
  * environment variables on your system always take precedence over those in `.env`

Once the environment variables are defined (either on your system or in `.env`),
you can initialize a Firebolt client with:

```python
from firebolt.client import FireboltClient

with FireboltClient() as fc:
    print(fc.settings)
```

Or you can configure the client by hand:
```python
from firebolt.client import FireboltClient
from firebolt.common.settings import Settings
from pydantic import SecretStr

with FireboltClient(settings=Settings(
   server="api.app.firebolt.io",
   user="email@domain.com",
   password=SecretStr("*****"),
   default_region="us-east-1",
)) as fc:
    print(fc.settings)
```

Under the hood, configuration works via Pydantic, 
see [here](https://pydantic-docs.helpmanual.io/usage/settings/).

### Contributing

See: [CONTRIBUTING.MD](CONTRIBUTING.MD)
