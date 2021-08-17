import httpx


def get_token(host: str, username: str, password: str) -> str:
    """
    Authenticate with username and password, and get a Bearer token.

    :param host:
    :param username:
    :param password:
    :return: Token
    """
    with httpx.Client(http2=True) as client:
        response = client.post(
            f"https://{host}/auth/v1/login",
            headers={"Content-Type": "application/json;charset=UTF-8"},
            json={"username": username, "password": password},
        )
        return response.json()["access_token"]


def get_http_client(host: str, username: str, password: str) -> httpx.Client:
    """
    Get an httpx client configured to talk to the Firebolt API.

    :param host: Firebolt server (eg. api.app.firebolt.io)
    :param username: Username, should be an entire email address
    :param password: Password
    :return: A configured httpx.Client
    """
    access_token = get_token(host=host, username=username, password=password)

    client = httpx.Client(http2=True, base_url=f"https://{host}")
    client.headers.update({"Authorization": f"Bearer {access_token}"})
    return client
