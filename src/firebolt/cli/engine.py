from collections import namedtuple
from typing import Optional

import typer
from tabulate import tabulate

from firebolt.model.database import Database
from firebolt.model.engine import Engine
from firebolt.model.instance_type import instance_types
from firebolt.model.provider import providers
from firebolt.model.region import regions

app = typer.Typer()


@app.command()
def create(
    name: str = typer.Argument(...),
    engine_type: str = typer.Argument(..., help="ingest|analytics"),
    description: str = None,
    region_name: str = None,
    compute_instance_type_name: str = None,
    compute_instance_count: str = None,
):
    getattr(Engine, f"create_{engine_type}")(
        name=name,
        description=description,
        region_name=region_name,
        compute_instance_type_name=compute_instance_type_name,
        compute_instance_count=compute_instance_count,
    )
    typer.secho(f"Engine {name} was successfully created", fg=typer.colors.GREEN)


@app.command()
def bind(
    engine_name: str = typer.Argument(...),
    database_name: str = typer.Argument(...),
):
    eng = Engine.get_by_name(engine_name)
    eng.bind_to_database(Database.get_by_name(database_name), False)
    typer.secho(
        f"Engine {engine_name} was successfully attached to {database_name}",
        fg=typer.colors.GREEN,
    )


EngineTable = namedtuple(
    "EngineTable",
    [
        "name",
        "provider_region",
        "status",
        "attached_to",
        "spec",
        "scale",
        "description",
    ],
)


def get_engine_content(eng: Engine) -> EngineTable:
    region = regions.get_by_id(
        eng.compute_region_key.region_id, eng.compute_region_key.provider_id
    )

    revision = eng.get_latest_engine_revision()

    provider = providers.providers_by_id[eng.compute_region_key.provider_id]

    return EngineTable(
        eng.name,
        "{} - {}".format(provider.name, region.name),
        eng.current_status_summary,
        eng.database.name if eng.database else "",
        instance_types.get_by_key(
            revision.specification.db_compute_instances_type_key
        ).name,
        revision.specification.db_compute_instances_count,
        eng.description,
    )


@app.command()
def get(name: str = typer.Argument(...)):
    eng = Engine.get_by_name(name)

    typer.echo("\n" + tabulate([get_engine_content(eng)], headers=EngineTable._fields))


@app.command()
def ls(
    database: Optional[str] = typer.Option(
        None, help="Filter engines based on database"
    ),
    jsonl: bool = typer.Option(
        False,
        help="Output results in new line delimited json. "
        "Default output format is a table",
    ),
):
    engines = Engine.list_engines()

    if not jsonl:
        data = []
        for eng in engines:
            if database and (not eng.database or eng.database.name != database):
                continue

            data.append(get_engine_content(eng))

        typer.echo("\n" + tabulate([d for d in data], headers=EngineTable._fields))
    else:
        typer.echo("\n".join(e.json() for e in engines))


@app.command()
def delete(name: str = typer.Argument(...)):
    eng = Engine.get_by_name(name)
    eng.delete()
    typer.secho(f"Engine: {name} was deleted", fg=typer.colors.RED)


@app.command()
def start(
    name: str = typer.Argument(...),
    wait: bool = typer.Option(
        True, help="Wait for engine start to complete before completing command"
    ),
):
    eng = Engine.get_by_name(name)
    eng.start(wait_for_startup=wait)


@app.command()
def stop(name: str = typer.Argument(...)):
    eng = Engine.get_by_name(name)
    eng.stop()
    typer.secho(f"Engine: {name} stopping request submitted")


@app.command()
def status(name: str = typer.Argument(...)):
    eng = Engine.get_by_name(name)
    eng_status = eng.current_status_summary
    typer.echo(f"Engine {name} status: {eng_status}")


@app.command()
def restart(name: str = typer.Argument(...)):
    Engine.get_by_name(name)
    # TODO add restart command
    # eng.restart()
