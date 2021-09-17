import typer
from tabulate import tabulate

from firebolt.model.engine import Engine

app = typer.Typer()


def data_to_arrays(data, cols):
    result = []
    for row in data:
        result.append([row[c] for c in cols])

    return result


@app.command()
def execute(
    engine_name: str = typer.Argument(...),
    query_text: str = typer.Argument(None, help="Inline query text to execution"),
    query_file: str = typer.Option(
        None, help="File location with query text to execute"
    ),
):
    if not query_text and not query_file:
        typer.secho("No quey provided", fg=typer.colors.RED)
        return

    if query_text:
        query_to_run = query_text
    else:
        query_to_run = open(query_file, "r").read()

    eng = Engine.get_by_name(engine_name)
    result = eng.run_query(query_to_run)

    cols = [h["name"] for h in result["meta"]]
    data_arrays = data_to_arrays(result["data"], cols)

    typer.echo("\n" + tabulate(data_arrays, headers=cols))
