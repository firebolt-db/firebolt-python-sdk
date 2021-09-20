# type: ignore

import typer

from firebolt.cli.configure import app as configure_app
from firebolt.cli.database import app as database_app
from firebolt.cli.engine import app as engine_app
from firebolt.cli.query import app as query_app
from firebolt.firebolt_client import FireboltClient

app = typer.Typer()

app.add_typer(database_app, name="database")
app.add_typer(engine_app, name="engine")
app.add_typer(query_app, name="query")
app.add_typer(configure_app, name="configure")


def main():
    with FireboltClient() as _:
        app()
