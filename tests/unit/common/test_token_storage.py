import os

from appdirs import user_config_dir
from pyfakefs.fake_filesystem import FakeFilesystem

from firebolt.common.token_storage import FernetEncrypter, TokenSecureStorage


def test_encrypter_happy_path():
    """
    Simple encrypt/decrypt using FernetEncrypter
    """
    salt = FernetEncrypter.generate_salt()
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
    salt1 = FernetEncrypter.generate_salt()
    salt2 = FernetEncrypter.generate_salt()

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


def test_token_storage_happy_path(fs: FakeFilesystem):
    """
    Test storage happy path cache token and get token
    """
    settings = {"username": "username", "password": "password"}
    assert TokenSecureStorage(**settings).get_cached_token() is None

    token = "some string to encrypt"
    TokenSecureStorage(**settings).cache_token(token)

    assert token == TokenSecureStorage(**settings).get_cached_token()
    token = "some new string to encrypt"

    TokenSecureStorage(**settings).cache_token(token)
    assert (
        token
        == TokenSecureStorage(
            username="username", password="password"
        ).get_cached_token()
    )


def test_token_storage_wrong_parameter(fs: FakeFilesystem):
    """
    Test getting token with different username or password
    """
    settings = {"username": "username", "password": "password"}
    token = "some string to encrypt"
    TokenSecureStorage(**settings).cache_token(token)

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
