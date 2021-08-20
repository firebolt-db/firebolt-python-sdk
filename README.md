# firebolt-sdk

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
2. Preferences -> Keymap -> External Tools -> lint, Assign the keyboard shortcut `Option-cmd-l`

### Generating Models

If you have a json response from the API, you can try running the following:
```python
engine_revision = {"something": "parsed_from_api"}

import pathlib
import json
with pathlib.Path("./engine_revision.json").open("w") as f:
    f.write(json.dumps(engine_revision))
```

From this file, you can [generate models](https://pydantic-docs.helpmanual.io/datamodel_code_generator/), like this:
```shell
pip install datamodel-code-generator
datamodel-codegen --input-file-type json --input engine_revision.json --output engine_revision.py
```

Or, you can try generating from the json api [spec](https://api.app.firebolt.io/docs/openapi.json):
```shell
datamodel-codegen --url https://api.app.firebolt.io/docs/openapi.json --output out/openapi.py
```

Note: this did not work for me, I suspect we may have to [dereference](https://github.com/koxudaxi/datamodel-code-generator/issues/500) first.
But it is a feature I would like to use in the future.

### Versioning

Consider adopting: 
 * https://packboard.atlassian.net/wiki/x/AYC6aQ
 * https://python-semantic-release.readthedocs.io/en/latest/ 