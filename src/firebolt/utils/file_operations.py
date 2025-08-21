from base64 import b64decode, urlsafe_b64encode
from hashlib import sha256
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend  # type: ignore
from cryptography.hazmat.primitives import hashes, padding  # type: ignore
from cryptography.hazmat.primitives.ciphers import (  # type: ignore
    Cipher,
    algorithms,
    modes,
)
from cryptography.hazmat.primitives.kdf.pbkdf2 import (
    PBKDF2HMAC,  # type: ignore
)


class FernetEncrypter:
    """PBKDF2HMAC based encrypter.

    Username and password combination is used as a key.

    Args:
        salt (str): Salt value for encryption
        username: Username for key
        password: Password for key
    """

    def __init__(self, salt: str, encryption_key: str):
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            salt=b64decode(salt),
            length=32,
            iterations=39000,
            backend=default_backend(),
        )
        self.fernet = Fernet(
            urlsafe_b64encode(kdf.derive(bytes(encryption_key, encoding="utf-8")))
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


def generate_salt() -> str:
    """Generate salt for FernetEncrypter.

    Returns:
        str: Generated salt
    """
    return "salt"


def generate_encrypted_file_name(cache_key: str, encryption_key: str) -> str:
    """Generate encrypted file name from cache key using AES encryption.

    Args:
        cache_key (str): The cache key to encrypt
        encryption_key (str): The encryption key

    Returns:
        str: Base64URL encoded AES encrypted filename ending in .txt
    """
    # Derive a 256-bit key from the encryption_key using PBKDF2
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        salt=b"firebolt_cache_salt",  # Fixed salt for deterministic key derivation
        length=32,  # 256 bits
        iterations=10000,
        backend=default_backend(),
    )
    aes_key = kdf.derive(encryption_key.encode("utf-8"))

    # Pad the cache_key to be a multiple of 16 bytes (AES block size)
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(cache_key.encode("utf-8"))
    padded_data += padder.finalize()

    # Use a fixed IV for deterministic encryption
    # (same input always produces same output)
    # This is acceptable for cache file names where we need deterministic results
    iv = sha256(cache_key.encode("utf-8")).digest()[:16]

    # Encrypt the padded cache_key
    cipher = Cipher(algorithms.AES(aes_key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

    # Base64URL encode the encrypted data and add .txt extension
    return urlsafe_b64encode(encrypted_data).decode("ascii").rstrip("=") + ".txt"
