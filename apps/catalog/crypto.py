"""Symmetric encryption for secrets stored in the database (SP-API refresh tokens)."""

import base64
import hashlib

from cryptography.fernet import Fernet
from django.conf import settings


def _fernet() -> Fernet:
    key = settings.FIELD_ENCRYPTION_KEY
    if not key:
        digest = hashlib.sha256(f"field-encryption:{settings.SECRET_KEY}".encode()).digest()
        key = base64.urlsafe_b64encode(digest).decode()
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt(value: str) -> str:
    return _fernet().encrypt(value.encode()).decode()


def decrypt(token: str) -> str:
    return _fernet().decrypt(token.encode()).decode()
