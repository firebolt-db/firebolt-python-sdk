# Resource Management
### Usage

See: [examples/management.ipynb](https://github.com/firebolt-db/firebolt-sdk/tree/main/examples/management.ipynb).

### Configuration

To use the SDK, you generally will want to set the following environment variables:
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

rm = ResourceManager()
print(rm.regions.default_region) # see your default region
```

Or you can configure settings manually:

```python
from firebolt.service.manager import ResourceManager
from firebolt.common.settings import Settings
from pydantic import SecretStr

rm = ResourceManager(settings=Settings(
    server="api.app.firebolt.io",
    user="email@domain.com",
    password=SecretStr("*****"),
    default_region="us-east-1",
))
print(rm.client.account_id) # see your account id
```

Under the hood, configuration works via Pydantic, 
see [here](https://pydantic-docs.helpmanual.io/usage/settings/).

### Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sdk/tree/main/CONTRIBUTING.MD)
