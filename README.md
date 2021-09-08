# firebolt-sdk

## Disclaimer
This project is incomplete and under heavy development. Things are changing fast!

Please feel free to try it out, but know that breaking changes may occur at any time.
If you have feature requests or feedback, please ask Eric Gustavson on slack.

To do:
* functionality: deletes/updates of engines/databases
* support for tables, indexes?
* unit tests
* consider a DI framework
* general polish
* documentation & examples

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
from firebolt.firebolt_client import FireboltClient

with FireboltClient() as fc:
    print(fc.settings)
```

Or you can configure the client by hand:
```python
from firebolt.firebolt_client import FireboltClient
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

### Links

* [REST API Docs](https://docs.firebolt.io/integrations/connecting-via-rest-api)
* [Dev Docs](https://api.dev.firebolt.io/devDocs)

### Contributing

1. Clone this repo into a virtual environment.
1. `pip install -e ".[dev]"`
1. `pre-commit install`
1. If using pycharm, set the test runner as `pytest` and mark `src` as a sources root.

Optionally setup Pycharm linting shortcut:

1. Preferences -> Tools -> External Tools > + (add new entry)
```
Name: lint
Description: Format the current file
Program: $PyInterpreterDirectory$/pre-commit
Arguments: run --files=$FilePath$Working 
Working Directory: $ProjectFileDir$
```
2. Preferences -> Keymap -> External Tools -> lint, 
   Assign the keyboard shortcut `Option-cmd-l`

### Before Committing

1. The pre-commit hook should catch linting errors
2. run `mypy src` to check for type errors
3. run `pytest` to run unit tests

Note: while there is a `mypy` hook for pre-commit, 
I found it too buggy to be worthwhile, so I just run mypy manually. 

### Docstrings

Use the Google format for docstrings. Do not include types or an indication 
of "optional" in docstrings. Those should be captured in the function signature 
as type annotations; no need to repeat them in the docstring.

Public methods and functions should have docstrings. 
One-liners are fine for simple methods and functions.

For PyCharm Users:

1. Tools > Python Integrated Tools > Docstring Format: "Google"
2. Editor > General > Smart Keys > Check "Insert documentation comment stub"
3. Editor > General > Smart Keys > Python > Uncheck "Insert type placeholders..."

### Method Order

In general, organize class internals in this order:

1. class attributes
2. `__init__()`
3. classmethods (`@classmethod`)
   * alternative constructors first
   * other classmethods next
4. properties (`@property`)
5. remaining methods 
   * put more important / broadly applicable functions first
   * group related functions together to minimize scrolling

Read more about this philosophy 
[here](https://softwareengineering.stackexchange.com/a/199317).

### Huge classes

If classes start to approach 1k lines, consider breaking them into parts, 
possibly like [this](https://stackoverflow.com/a/47562412).

### Generating Models

If you have a json response from the API, you can try running the following:
```python
engine_revision = {"something": "parsed_from_api"}

import pathlib
import json
with pathlib.Path("./engine_revision.json").open("w") as f:
    f.write(json.dumps(engine_revision))
```

From this file, you can [generate models](
https://pydantic-docs.helpmanual.io/datamodel_code_generator/), like this:
```shell
pip install datamodel-code-generator
datamodel-codegen \
   --input-file-type json \
   --input engine_revision.json \
   --output engine_revision.py
```

You can also generate code from a json string in python:
```python
from datamodel_code_generator import generate, InputFileType
import json
instance = {'name': 'r5.8xlarge'}
instance_json = json.dumps(instance)

# calling generate will print out the generated code
generate(
    input_=instance_json,
    input_file_type=InputFileType.Json
)
```

Or, you can try generating from the json api [spec](
https://api.app.firebolt.io/docs/openapi.json):
```shell
datamodel-codegen \
  --url https://api.app.firebolt.io/docs/openapi.json \
  --output out/openapi.py
```

Note: this did not work for me, I suspect we may have to [dereference](
https://github.com/koxudaxi/datamodel-code-generator/issues/500) first.
But it is a feature I would like to use in the future.

### Versioning

Consider adopting: 
 * https://packboard.atlassian.net/wiki/x/AYC6aQ
 * https://python-semantic-release.readthedocs.io/en/latest/
