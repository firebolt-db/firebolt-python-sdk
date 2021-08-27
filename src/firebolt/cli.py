# type: ignore

import json

import typer

from .firebolt_client import FireboltClient
from .model.engine import Engine

app = typer.Typer()

ENGINE_OPERATIONS = ["start", "stop", "restart"]
READ_COMMANDS = ["ls", "list", "read"]
DELETE_COMMANDS = ["delete", "destroy"]
CREATE_COMMANDS = ["create"]


def run():
    with FireboltClient.from_env() as _:
        app()


@app.command()
def engine(action: str, name: str = None):
    if action in READ_COMMANDS:
        if name:
            eng = Engine.get_by_name(name)
            typer.echo(eng.json())
        else:
            engines = Engine.list_engines()
            typer.echo(json.dumps(engines))
    elif action in CREATE_COMMANDS:
        pass
    elif action in DELETE_COMMANDS:
        eng = Engine.get_by_name(name)
        eng.delete()
        typer.secho(f"Engine: {name} was deleted", fg=typer.colors.RED)
    elif action in ENGINE_OPERATIONS:
        eng = Engine.get_by_name(name)
        getattr(eng, action)()
    else:
        raise Exception(f"Engine command: {action} not supported")


# @app.command()
# def database(action: str, name: str = None):
#     pass
