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
```

* You can store these in a `.env` file 
* environment variables on your system always take precedence over those in `.env`

Once the environment variables are defined (either on your system or in `.env`),
you can initialize a ResourceManager with:

```python
from firebolt.service.manager import ResourceManager

with ResourceManager() as rm:
    print(rm.regions.default_region) # see your default region
```

Or you can configure settings manually:

```python
from firebolt.service.manager import ResourceManager
from firebolt.common.settings import Settings
from pydantic import SecretStr

with ResourceManager(settings=Settings(
        server="api.app.firebolt.io",
        user="email@domain.com",
        password=SecretStr("*****"),
        default_region="us-east-1",
)) as rm:
    print(rm.client.account_id) # see your account id
```

Under the hood, configuration works via Pydantic, 
see [here](https://pydantic-docs.helpmanual.io/usage/settings/).

### Contributing

See: [CONTRIBUTING.MD](CONTRIBUTING.MD)
