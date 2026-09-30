"""Settings for the test suite: debug defaults, throwaway data dir."""

import os
import tempfile

os.environ.setdefault("DJANGO_DEBUG", "1")
os.environ.setdefault("DATA_DIR", os.path.join(tempfile.gettempdir(), "aws-reports-tests"))
os.environ.setdefault("DATABASE_URL", "sqlite://:memory:")

from .settings import *  # noqa: E402,F403

PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
STORAGES["staticfiles"] = {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"}  # noqa: F405
os.makedirs(STATIC_ROOT, exist_ok=True)  # noqa: F405
