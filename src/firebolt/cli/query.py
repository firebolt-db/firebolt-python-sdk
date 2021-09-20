from typing import List, Optional

import typer
from tabulate import tabulate

from firebolt.model.engine import Engine

app = typer.Typer()


def data_to_arrays(data, cols):
    result = []
    for row in data:
        result.append([row[c] for c in cols])

    return result


def print_query_results(result):
    cols = [h["name"] for h in result["meta"]]
    data_arrays = data_to_arrays(result["data"], cols)

    typer.echo("\n" + tabulate(data_arrays, headers=cols))


@app.command()
def execute(
    engine_name: str = typer.Argument(...),
    query_text: str = typer.Argument(None, help="Inline query text to execution"),
    query_file: str = typer.Option(
        None, help="File location with query text to execute"
    ),
    run_async: bool = typer.Option(False, help="Run query in async mode"),
    setting: Optional[List[str]] = typer.Option(
        None, help="Optional other settings provided setting=1 syntax"
    ),
):

    settings_as_dict = {t.split("=")[0]: t.split("=")[1] for t in setting}

    if not query_text and not query_file:
        typer.secho("No quey provided", fg=typer.colors.RED)
        return

    if query_text:
        query_to_run = query_text
    else:
        query_to_run = open(query_file, "r").read()

    eng = Engine.get_by_name(engine_name)
    result = eng.run_query(
        query_to_run, async_execution=run_async, settings=settings_as_dict
    )

    if run_async:
        typer.echo("Query ID: {}".format(result["query_id"]))
    else:
        print_query_results(result)


@app.command()
def status(
    engine_name: str = typer.Argument(...),
    query_id: str = typer.Option(None),
):
    sql = """
SELECT
    "START_TIME",
    "DURATION",
    "STATUS",
    "QUERY_ID",
    "QUERY_TEXT"
FROM
    catalog.query_history
WHERE TRUE
AND {query_filter}
AND "QUERY_TEXT" NOT LIKE '%query_history%'
ORDER BY "START_TIME" DESC
LIMIT 100
"""

    eng = Engine.get_by_name(engine_name)

    sql_to_run = sql.format(
        query_filter=f"QUERY_ID = '{query_id}'" if query_id else "TRUE"
    )
    result = eng.run_query(sql_to_run)

    print_query_results(result)
