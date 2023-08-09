# Resource Management
### Usage

See: [examples/management.ipynb](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/examples/management.ipynb).

### Configuration

To use the SDK, you generally will want to set the following environment variables:
```
FIREBOLT_USER='email@domain.com'
FIREBOLT_PASSWORD='*****'
FIREBOLT_ACCOUNT='my_account'
FIREBOLT_SERVER='api.app.firebolt.io'
FIREBOLT_DEFAULT_REGION='us-east-1'
```

Once the environment variables are defined,
you can initialize a ResourceManager with:

```python
from firebolt.service.manager import ResourceManager

rm = ResourceManager()
print(rm.regions.default_region) # see your default region
```

Or you can configure settings manually:

```python
from firebolt.client.auth import UsernamePassword
from firebolt.service.manager import ResourceManager
from firebolt.common.settings import Settings

rm = ResourceManager(settings=Settings(
    auth=UsernamePassword("email@domain.com", "*****")
    account_name="account", # Necessary if you have multiple accounts.
    server="api.app.firebolt.io",
    default_region="us-east-1",
))
print(rm.client.account_id) # see your account id
```

### Contributing

See: [CONTRIBUTING.MD](https://github.com/firebolt-db/firebolt-sdk/tree/0.x/CONTRIBUTING.MD)
