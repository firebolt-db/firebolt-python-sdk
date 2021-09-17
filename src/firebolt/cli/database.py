import typer
from tabulate import tabulate

from firebolt.model.database import Database
from firebolt.model.provider import providers
from firebolt.model.region import regions

app = typer.Typer()


@app.command()
def create(
    name: str = typer.Argument(...),
    region_name: str = typer.Argument(..., help="Region for database e.g. us-east-1"),
):
    Database.create_new(name, region_name)
    typer.secho(f"Database successfully created", fg=typer.colors.GREEN)


HEADERS = ["name", "provider - region", "data_size", "description"]


def get_database_content(db):
    region = regions.get_by_id(
        db.compute_region_key.region_id, db.compute_region_key.provider_id
    )

    provider = providers.providers_by_id[db.compute_region_key.provider_id]

    return [
        db.name,
        "{} - {}".format(provider.name, region.name),
        db.data_size_compressed,
        db.description,
    ]


@app.command()
def get(name: str = typer.Argument(...)):
    db = Database.get_by_name(name)

    typer.echo("\n" + tabulate([get_database_content(db)], headers=HEADERS))


@app.command()
def ls(
    jsonl: bool = typer.Option(
        False,
        help="Output results in new line delimited json. "
        "Default output format is a table",
    ),
):
    databases = Database.list_databases()
    if not jsonl:
        data = []
        for db in databases:
            data.append(get_database_content(db))

        typer.echo("\n" + tabulate(data, headers=HEADERS))
    else:
        typer.echo("\n".join(d.json() for d in databases))


@app.command()
def delete(
    name: str = typer.Argument(...),
):
    database_name = typer.prompt("Confirm deletion, type name of database")
    if database_name == name:
        db = Database.get_by_name(name)
        Database.delete(db)
        typer.secho("Database successfully deleted", fg=typer.colors.RED)
    else:
        typer.echo(
            f"Names did not match between command: {name} and confirmation: "
            f"{database_name}. Not deleting"
        )
