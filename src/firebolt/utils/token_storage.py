from base64 import b64decode, b64encode, urlsafe_b64encode
from hashlib import sha256
from json import JSONDecodeError
from json import dump as json_dump
from json import load as json_load
from os import makedirs, path, urandom
from time import time
from typing import Optional

from appdirs import user_data_dir
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend  # type: ignore
from cryptography.hazmat.primitives import hashes  # type: ignore
from cryptography.hazmat.primitives.kdf.pbkdf2 import (
    PBKDF2HMAC,  # type: ignore
)

APPNAME = "firebolt"


def generate_salt() -> str:
    """Generate salt for FernetExcrypter.

    Returns:
        str: Generated salt
    """
    return b64encode(urandom(16)).decode("ascii")


def generate_file_name(username: str, password: str) -> str:
    """Generate unique file name based on username and password.

    Username and password values are not exposed.

    Args:
        username (str): Username
        password (str): Password

    Returns:
        str: File name 64 characters long

    """
    username_hash = sha256(username.encode("utf-8")).hexdigest()[:32]
    password_hash = sha256(password.encode("utf-8")).hexdigest()[:32]

    return f"{username_hash}{password_hash}.json"


class TokenSecureStorage:
    """File system storage for token.

    Token is encrypted using username and password.

    Args:
        username (str): Username
        password (str): Password
    """

    def __init__(self, username: str, password: str):
        self._data_dir = user_data_dir(appname=APPNAME)
        makedirs(self._data_dir, exist_ok=True)

        self._token_file = path.join(
            self._data_dir, generate_file_name(username, password)
        )

        self.salt = self._get_salt()
        self.encrypter = FernetEncrypter(self.salt, username, password)

    def _get_salt(self) -> str:
        """Get salt from the file if exists, or generate a new one.

        Returns:
            str: Salt
        """
        res = self._read_data_json()
        return res.get("salt", generate_salt())

    def _read_data_json(self) -> dict:
        """Read json token file.

        Returns:
            dict: JSON object
        """
        if not path.exists(self._token_file):
            return {}

        with open(self._token_file) as f:
            try:
                return json_load(f)
            except JSONDecodeError:
                return {}

    def get_cached_token(self) -> Optional[str]:
        """Get decrypted token.

        If token is not found, cannot be decrypted with username and password,
        or is expired - None will be returned.

        Returns:
            Optional[str]: Decrypted token or None
        """
        res = self._read_data_json()
        if "token" not in res:
            return None

        # Ignore expired tokens
        if "expiration" in res and res["expiration"] <= int(time()):
            return None

        return self.encrypter.decrypt(res["token"])

    def cache_token(self, token: str, expiration_ts: int) -> None:
        """Encrypt and store token in file system.

        Expiration timestamp is also stored with token in order to later
        be able to check if it's expired.

        Args:
            token (str): Token to store
            expiration_ts (int): Token expiration timestamp
        """
        token = self.encrypter.encrypt(token)

        with open(self._token_file, "w") as f:
            json_dump(
                {"token": token, "salt": self.salt, "expiration": expiration_ts}, f
            )


class FernetEncrypter:
    """PBDKF2HMAC based encrypter.

    Username and password combination is used as a key.

    Args:
        salt (str): Salt value for encryption
        username: Username for key
        password: Password for key
    """

    def __init__(self, salt: str, username: str, password: str):
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            salt=b64decode(salt),
            length=32,
            iterations=39000,
            backend=default_backend(),
        )
        self.fernet = Fernet(
            urlsafe_b64encode(
                kdf.derive(bytes(f"{username}{password}", encoding="utf-8"))
            )
        )

    def encrypt(self, data: str) -> str:
        """Encrypt data string.

        Args:
            data (str): Data for encryption

        Returns:
            str: Encrypted data

        """
        return self.fernet.encrypt(bytes(data, encoding="utf-8")).decode("utf-8")

    def decrypt(self, data: str) -> Optional[str]:
        """Decrypt encrypted data.

        Args:
            data (str): Encrypted data

        Returns:
            Optional[str]: Decrypted data

        """
        try:
            return self.fernet.decrypt(bytes(data, encoding="utf-8")).decode("utf-8")
        except InvalidToken:
            return None
