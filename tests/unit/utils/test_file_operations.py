from firebolt.utils.file_operations import (
    FernetEncrypter,
    generate_encrypted_file_name,
    generate_salt,
)


def test_generate_encrypted_file_name_returns_same_value():
    # Test the function with sample inputs
    assert generate_encrypted_file_name(
        "test_key", "test_encryption_key"
    ) == generate_encrypted_file_name("test_key", "test_encryption_key")


def test_generate_encrypted_file_name_different_inputs_different_outputs():
    # Test that different inputs produce different outputs
    result1 = generate_encrypted_file_name("test_key1", "test_encryption_key")
    result2 = generate_encrypted_file_name("test_key2", "test_encryption_key")
    assert result1 != result2


def test_generate_encrypted_file_name_different_keys_different_outputs():
    # Test that different encryption keys produce different outputs
    result1 = generate_encrypted_file_name("test_key", "test_encryption_key1")
    result2 = generate_encrypted_file_name("test_key", "test_encryption_key2")
    assert result1 != result2


def test_generate_encrypted_file_name_format():
    # Test that the output has the correct format
    result = generate_encrypted_file_name("test_key", "test_encryption_key")
    assert result.endswith(".txt")
    assert len(result) > 10  # Should be a reasonable length with .txt extension


def test_generate_salt():
    """Test salt generation."""
    salt = generate_salt()
    assert salt == "salt"


def test_fernet_encrypter():
    """Test FernetEncrypter encryption and decryption."""
    salt = generate_salt()
    encryption_key = "test_encryption_key"

    encrypter = FernetEncrypter(salt, encryption_key)

    test_data = "Hello, World! This is test data."

    # Encrypt data
    encrypted = encrypter.encrypt(test_data)
    assert encrypted != test_data
    assert len(encrypted) > len(test_data)

    # Decrypt data
    decrypted = encrypter.decrypt(encrypted)
    assert decrypted == test_data


def test_fernet_encrypter_invalid_data():
    """Test FernetEncrypter with invalid encrypted data."""
    salt = generate_salt()
    encryption_key = "test_encryption_key"

    encrypter = FernetEncrypter(salt, encryption_key)

    # Try to decrypt invalid data
    result = encrypter.decrypt("invalid_encrypted_data")
    assert result is None


def test_fernet_encrypter_different_keys():
    """Test that different keys produce different encrypted data."""
    salt = generate_salt()
    encryption_key1 = "test_key_1"
    encryption_key2 = "test_key_2"

    encrypter1 = FernetEncrypter(salt, encryption_key1)
    encrypter2 = FernetEncrypter(salt, encryption_key2)

    test_data = "Hello, World!"

    encrypted1 = encrypter1.encrypt(test_data)
    encrypted2 = encrypter2.encrypt(test_data)

    # Different keys should produce different encrypted data
    assert encrypted1 != encrypted2

    # Each encrypter should decrypt its own data correctly
    assert encrypter1.decrypt(encrypted1) == test_data
    assert encrypter2.decrypt(encrypted2) == test_data

    # But shouldn't be able to decrypt the other's data
    assert encrypter1.decrypt(encrypted2) is None
    assert encrypter2.decrypt(encrypted1) is None
