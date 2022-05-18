import os
from unittest.mock import patch

from appdirs import user_config_dir
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt.utils.token_storage import (
    FernetEncrypter,
    TokenSecureStorage,
    generate_salt,
)


def test_encrypter_happy_path():
    """
    Simple encrypt/decrypt using FernetEncrypter
    """
    salt = generate_salt()
    encrypter1 = FernetEncrypter(salt, username="username", password="password")
    encrypter2 = FernetEncrypter(salt, username="username", password="password")

    token = "some string to encrypt"
    encrypted_token = encrypter1.encrypt(token)

    assert token == encrypter2.decrypt(encrypted_token)


def test_encrypter_wrong_parameter():
    """
    Test that decryption only works, if the correct salt,
    username and password is provided, otherwise None is returned
    """
    salt1 = generate_salt()
    salt2 = generate_salt()

    encrypter1 = FernetEncrypter(salt1, username="username", password="password")

    token = "some string to encrypt"
    encrypted_token = encrypter1.encrypt(token)

    encrypter2 = FernetEncrypter(salt2, username="username", password="password")
    assert encrypter2.decrypt(encrypted_token) is None

    encrypter2 = FernetEncrypter(salt1, username="username1", password="password")
    assert encrypter2.decrypt(encrypted_token) is None

    encrypter2 = FernetEncrypter(salt1, username="username", password="password1")
    assert encrypter2.decrypt(encrypted_token) is None

    encrypter2 = FernetEncrypter(salt1, username="username", password="password")
    assert encrypter2.decrypt(encrypted_token) == token


@patch("firebolt.utils.token_storage.time", return_value=0)
def test_token_storage_happy_path(fs: FakeFilesystem):
    """
    Test storage happy path cache token and get token
    """
    settings = {"username": "username", "password": "password"}
    assert TokenSecureStorage(**settings).get_cached_token() is None

    token = "some string to encrypt"
    TokenSecureStorage(**settings).cache_token(token, 1)

    assert token == TokenSecureStorage(**settings).get_cached_token()
    token = "some new string to encrypt"

    TokenSecureStorage(**settings).cache_token(token, 1)
    assert token == TokenSecureStorage(**settings).get_cached_token()


@patch("firebolt.utils.token_storage.time", return_value=0)
def test_token_storage_wrong_parameter(fs: FakeFilesystem):
    """
    Test getting token with different username or password
    """
    settings = {"username": "username", "password": "password"}
    token = "some string to encrypt"
    TokenSecureStorage(**settings).cache_token(token, 1)

    assert (
        TokenSecureStorage(
            username="username", password="wrong_password"
        ).get_cached_token()
        is None
    )
    assert (
        TokenSecureStorage(
            username="wrong_username", password="password"
        ).get_cached_token()
        is None
    )
    assert TokenSecureStorage(**settings).get_cached_token() == token


def test_token_storage_json_broken(fs: FakeFilesystem):
    """
    Check that the TokenSecureStorage properly handles broken json
    """
    settings = {"username": "username", "password": "password"}

    data_dir = os.path.join(user_config_dir(), "firebolt")
    fs.create_dir(data_dir)
    fs.create_file(os.path.join(data_dir, "token.json"), contents="{Not a valid json")

    assert TokenSecureStorage(**settings).get_cached_token() is None


@patch("firebolt.utils.token_storage.time", return_value=0)
def test_multiple_tokens(fs: FakeFilesystem) -> None:
    """
    Check that the TokenSecureStorage properly handles multiple tokens hashed
    """
    settings1 = {"username": "username1", "password": "password1"}
    settings2 = {"username": "username2", "password": "password2"}
    token1 = "token1"
    token2 = "token2"
    token3 = "token3"

    st1 = TokenSecureStorage(**settings1)
    st2 = TokenSecureStorage(**settings2)

    st1.cache_token(token1, 1)

    assert st1.get_cached_token() == token1
    assert st2.get_cached_token() is None

    st2.cache_token(token2, 1)

    assert st1.get_cached_token() == token1
    assert st2.get_cached_token() == token2

    st1.cache_token(token3, 1)
    assert st1.get_cached_token() == token3
    assert st2.get_cached_token() == token2


@patch("firebolt.utils.token_storage.time", return_value=0)
def test_expired_token(fs: FakeFilesystem) -> None:
    """
    Check that TokenSecureStorage ignores expired tokens
    """
    tss = TokenSecureStorage(username="username", password="password")
    tss.cache_token("token", 0)

    assert tss.get_cached_token() is None
