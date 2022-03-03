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
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

APPNAME = "firebolt"


def generate_salt() -> str:
    return b64encode(urandom(16)).decode("ascii")


def generate_file_name(username: str, password: str) -> str:
    username_hash = sha256(username.encode("utf-8")).hexdigest()[:32]
    password_hash = sha256(password.encode("utf-8")).hexdigest()[:32]

    return f"{username_hash}{password_hash}.json"


class TokenSecureStorage:
    def __init__(self, username: str, password: str):
        """
        Class for permanent storage of token in the filesystem in encrypted way

        :param username: username used for toke encryption
        :param password: password used for toke encryption
        """
        self._data_dir = user_data_dir(appname=APPNAME)
        makedirs(self._data_dir, exist_ok=True)

        self._token_file = path.join(
            self._data_dir, generate_file_name(username, password)
        )

        self.salt = self._get_salt()
        self.encrypter = FernetEncrypter(self.salt, username, password)

    def _get_salt(self) -> str:
        """
        Get salt from the file if exists, or generate a new one

        :return: salt
        """
        res = self._read_data_json()
        return res.get("salt", generate_salt())

    def _read_data_json(self) -> dict:
        """
        Read json token file

        :return: json object as dict
        """
        if not path.exists(self._token_file):
            return {}

        with open(self._token_file) as f:
            try:
                return json_load(f)
            except JSONDecodeError:
                return {}

    def get_cached_token(self) -> Optional[str]:
        """
        Get decrypted token using username and password
        If the token not found or token cannot be decrypted using username, password
        None will be returned

        :return: token or None
        """
        res = self._read_data_json()
        if "token" not in res:
            return None

        # Ignore expired tokens
        if "expiration" in res and res["expiration"] <= int(time()):
            return None

        return self.encrypter.decrypt(res["token"])

    def cache_token(self, token: str, expiration_ts: int) -> None:
        """

        :param token:
        :return:
        """
        token = self.encrypter.encrypt(token)

        with open(self._token_file, "w") as f:
            json_dump(
                {"token": token, "salt": self.salt, "expiration": expiration_ts}, f
            )


class FernetEncrypter:
    def __init__(self, salt: str, username: str, password: str):
        """

        :param salt:
        :param username:
        :param password:
        """

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            salt=b64decode(salt),
            length=32,
            iterations=39000,
        )
        self.fernet = Fernet(
            urlsafe_b64encode(
                kdf.derive(bytes(f"{username}{password}", encoding="utf-8"))
            )
        )

    def encrypt(self, data: str) -> str:
        return self.fernet.encrypt(bytes(data, encoding="utf-8")).decode("utf-8")

    def decrypt(self, data: str) -> Optional[str]:
        try:
            return self.fernet.decrypt(bytes(data, encoding="utf-8")).decode("utf-8")
        except InvalidToken:
            return None
