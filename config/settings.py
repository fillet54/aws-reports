"""
Django settings for the Amazon reporting app.

Everything deployment-specific comes from environment variables so the same
settings work for local development, tests and the Docker image.
"""

import os
from pathlib import Path

import dj_database_url

BASE_DIR = Path(__file__).resolve().parent.parent


def env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_list(name: str, default: str = "") -> list[str]:
    return [item.strip() for item in env(name, default).split(",") if item.strip()]


DEBUG = env_bool("DJANGO_DEBUG", default=False)

SECRET_KEY = env("DJANGO_SECRET_KEY")
if not SECRET_KEY:
    if not DEBUG:
        raise RuntimeError("DJANGO_SECRET_KEY must be set when DJANGO_DEBUG is off.")
    SECRET_KEY = "dev-only-insecure-secret-key"

ALLOWED_HOSTS = env_list("DJANGO_ALLOWED_HOSTS", "localhost,127.0.0.1,[::1]")
CSRF_TRUSTED_ORIGINS = env_list("DJANGO_CSRF_TRUSTED_ORIGINS")

# All mutable state (SQLite DB when used, raw report files) lives under DATA_DIR.
DATA_DIR = Path(env("DATA_DIR", str(BASE_DIR / "var")))
DATA_DIR.mkdir(parents=True, exist_ok=True)

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "whitenoise.runserver_nostatic",
    "django.contrib.staticfiles",
    "django.contrib.humanize",
    "apps.accounts",
    "apps.catalog",
    "apps.sales",
    "apps.reports",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    # Every view requires login unless explicitly marked @login_not_required.
    "django.contrib.auth.middleware.LoginRequiredMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "apps.accounts.context_processors.navigation",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": dj_database_url.parse(
        env("DATABASE_URL", f"sqlite:///{DATA_DIR / 'db.sqlite3'}"),
        conn_max_age=60,
    )
}
if DATABASES["default"]["ENGINE"] == "django.db.backends.sqlite3":
    # WAL lets the sync worker write while the web process reads.
    DATABASES["default"].setdefault("OPTIONS", {})
    DATABASES["default"]["OPTIONS"]["init_command"] = "PRAGMA journal_mode=WAL;"
    DATABASES["default"]["OPTIONS"]["transaction_mode"] = "IMMEDIATE"

AUTH_USER_MODEL = "accounts.User"

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "home"
LOGOUT_REDIRECT_URL = "login"

LANGUAGE_CODE = "en-us"
# Day/week/month boundaries in reports are computed in this time zone, and
# times are displayed in it. Amazon US uses Pacific time for its business days.
# (Data is always stored in UTC.)
REPORT_TIME_ZONE = env("REPORT_TIME_ZONE", "America/Los_Angeles")
TIME_ZONE = REPORT_TIME_ZONE
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = DATA_DIR / "staticfiles"
STATICFILES_DIRS = [BASE_DIR / "static"]
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "whitenoise.storage.CompressedManifestStaticFilesStorage"},
}

MEDIA_ROOT = DATA_DIR / "media"
MEDIA_URL = "media/"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# ---------------------------------------------------------------------------
# Amazon Selling Partner API
# ---------------------------------------------------------------------------

# "dummy" generates realistic fake orders; "sp_api" talks to Amazon.
AMAZON_CLIENT = env("AMAZON_CLIENT", "dummy")
SP_API_LWA_APP_ID = env("SP_API_LWA_APP_ID", "")
SP_API_LWA_CLIENT_SECRET = env("SP_API_LWA_CLIENT_SECRET", "")

# Key used to encrypt per-brand refresh tokens at rest. Falls back to a key
# derived from SECRET_KEY; set it explicitly in production so rotating
# SECRET_KEY doesn't make stored tokens unreadable.
FIELD_ENCRYPTION_KEY = env("FIELD_ENCRYPTION_KEY", "")

# ---------------------------------------------------------------------------
# Production hardening (enable once served over HTTPS)
# ---------------------------------------------------------------------------

if env_bool("DJANGO_HTTPS", default=False):
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
    SESSION_COOKIE_SECURE = True
    CSRF_COOKIE_SECURE = True
    SECURE_HSTS_SECONDS = 60 * 60 * 24 * 30

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": env("LOG_LEVEL", "INFO")},
}
