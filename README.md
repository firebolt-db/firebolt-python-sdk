# firebolt-sdk

## Disclaimer
This project is incomplete and under heavy development. Things are changing fast!

Please feel free to try it out, but know that breaking changes may occur at any time.
If you have feature requests or feedback, please ask Eric Gustavson on slack.

To do:
* functionality: query, bindings, deletes/updates of engines/databases
* support for tables, indexes?
* unit tests
* consider a DI framework
* general polish
* documentation & examples

### Links

* [REST API Docs](https://docs.firebolt.io/integrations/connecting-via-rest-api)
* [Dev Docs](https://api.dev.firebolt.io/devDocs)

### Contributing

1. Clone this repo into a virtual environment.
1. `pip install -e ".[dev]"`
1. `pre-commit install`
1. If using pycharm, set the test runner as `pytest` and mark `src` as a sources root.

Optionally configure Pycharm linting shortcut:

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

### Docstrings

Use the Google format for docstrings. Do not include types or an indication 
of "optional" in docstrings. Those should be captured in the function signature 
as type annotations; no need to repeat them in the docstring.

Public methods and functions should have docstrings. 
One-liners are fine for simple methods and functions.

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
