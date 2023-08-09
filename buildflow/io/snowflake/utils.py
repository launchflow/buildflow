from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def read_private_key_file(file_path: str) -> str:
    with open(file_path, "rb") as pem_in:
        p_key = serialization.load_pem_private_key(
            pem_in.read(),
            password=None,
            backend=default_backend(),
        )

    return p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def read_private_key_file_bytes(file_path: str) -> str:
    with open(file_path, "rb") as pem_in:
        p_key = serialization.load_pem_private_key(
            pem_in.read(),
            password=None,
            backend=default_backend(),
        )

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
