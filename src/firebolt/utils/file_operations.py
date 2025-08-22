from base64 import b64decode, b64encode, urlsafe_b64encode
from hashlib import sha256
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend  # type: ignore
from cryptography.hazmat.primitives import hashes  # type: ignore
from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
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
    """Generate encrypted file name from cache key using AES-GCM encryption.

    This implementation matches the Java EncryptionService to ensure compatibility.

    Args:
        cache_key (str): The cache key to encrypt
        encryption_key (str): The encryption key

    Returns:
        str: Base64 encoded AES-GCM encrypted filename
    """
    # Derive AES key using SHA-256
    key_hash = sha256(encryption_key.encode("utf-8")).digest()
    aes_key = key_hash[:32]  # Use first 32 bytes for AES-256

    # Generate deterministic nonce
    nonce_input = (encryption_key + encryption_key).encode("utf-8")
    nonce_hash = sha256(nonce_input).digest()
    nonce = nonce_hash[:12]  # AES-GCM nonce should be 12 bytes

    # Encrypt using AES-GCM
    aesgcm = AESGCM(aes_key)
    encrypted_data = aesgcm.encrypt(nonce, cache_key.encode("utf-8"), None)

    # Base64 encode
    return b64encode(encrypted_data).decode("ascii")
