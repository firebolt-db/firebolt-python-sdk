import base64
import json
import os
from json import JSONDecodeError
from typing import Optional

from appdirs import user_data_dir
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class TokenSecureStorage:
    def __init__(self, username: str, password: str):
        """
        Class for permanent storage of token in the filesystem in encrypted way

        :param username: username used for toke encryption
        :param password: password used for toke encryption
        """
        self._data_dir = user_data_dir(appname="firebolt")
        os.makedirs(self._data_dir, exist_ok=True)

        self._token_file = os.path.join(self._data_dir, "token.json")

        self.salt = self._get_salt()
        self.encrypter = FernetEncrypter(self.salt, username, password)

    def _get_salt(self) -> str:
        """
        Get salt from the file if exists, or generate a new one

        :return: salt
        """
        res = self._read_data_json()
        if "salt" not in res:
            return FernetEncrypter.generate_salt()
        else:
            return res["salt"]

    def _read_data_json(self) -> dict:
        """
        Read json file token.json

        :return: json object as dict
        """
        if not os.path.exists(self._token_file):
            return {}

        with open(self._token_file) as f:
            try:
                return json.load(f)
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

        return self.encrypter.decrypt(res["token"])

    def cache_token(self, token: str) -> None:
        """

        :param token:
        :return:
        """
        token = self.encrypter.encrypt(token)

        with open(self._token_file, "w") as f:
            json.dump({"token": token, "salt": self.salt}, f)


class FernetEncrypter:
    def __init__(self, salt: str, username: str, password: str):
        """

        :param salt:
        :param username:
        :param password:
        """

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            salt=base64.b64decode(salt),
            length=32,
            iterations=39000,
        )
        self.fernet = Fernet(
            base64.urlsafe_b64encode(
                kdf.derive(bytes(f"{username}{password}", encoding="utf-8"))
            )
        )

    @staticmethod
    def generate_salt() -> str:
        return base64.b64encode(os.urandom(16)).decode("ascii")

    def encrypt(self, data: str) -> str:
        return self.fernet.encrypt(bytes(data, encoding="utf-8")).decode("utf-8")

    def decrypt(self, data: str) -> Optional[str]:
        try:
            return self.fernet.decrypt(bytes(data, encoding="utf-8")).decode("utf-8")
        except InvalidToken:
            return None
