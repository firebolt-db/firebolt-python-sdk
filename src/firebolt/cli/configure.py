import os
from typing import Optional

import typer

app = typer.Typer()

ENV_FILE = ".env"


@app.command()
def env(
    firebolt_user: Optional[str] = typer.Option(..., prompt=True),
    firebolt_password: Optional[str] = typer.Option(
        ..., prompt=True, hide_input=True, help="PW hidden while typing"
    ),
    firebolt_server: Optional[str] = typer.Option(..., prompt=True),
    firebolt_default_region: Optional[str] = typer.Option(..., prompt=True),
    firebolt_default_provider: Optional[str] = typer.Option(..., prompt=True),
    update_env: Optional[bool] = typer.Option(
        True, help="Whether or not to update env variables. Default true"
    ),
):
    current_settings = {}
    try:
        with open(ENV_FILE, "r") as env_file:
            for line in env_file:
                current_settings[line.split("=")[0]] = line.split("=")[1].replace(
                    "\n", ""
                )
    except FileNotFoundError:
        current_settings

    fields = [
        "FIREBOLT_USER",
        "FIREBOLT_PASSWORD",
        "FIREBOLT_SERVER",
        "FIREBOLT_DEFAULT_REGION",
        "FIREBOLT_DEFAULT_PROVIDER",
    ]

    new_contents = ""
    for field in fields:
        val = "{}=".format(field)
        if eval(field.lower()).strip():
            val += "'{}'".format(eval(field.lower()).strip())
        else:
            val += current_settings.get(field, "")

        new_contents += val + "\n"

        if update_env:
            # write out the new settings to environment variables
            os.environ[field] = val

    with open(ENV_FILE, "w") as new_env:
        new_env.write(new_contents)
