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

### Versioning

Consider adopting: 
 * https://packboard.atlassian.net/wiki/x/AYC6aQ
 * https://python-semantic-release.readthedocs.io/en/latest/ 