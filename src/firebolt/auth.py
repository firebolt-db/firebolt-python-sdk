import getpass

import keyring
import requests

from firebolt.config import CONFIG


def auth():
    headers = {"Content-Type": "application/json;charset=UTF-8"}

    username = "eg@firebolt.io"
    host = CONFIG["dev"]["host"]

    password = keyring.get_password(service_name=host, username=username)
    if not password:
        password = getpass.getpass(prompt=f"Password for {username} on {host}: ")
        keyring.set_password(service_name=host, username=username, password=password)
    data = {"username": username, "password": password}

    response = requests.post(
        f"https://{host}/auth/v1/login", headers=headers, json=data
    )

    print(response.json())


auth()
