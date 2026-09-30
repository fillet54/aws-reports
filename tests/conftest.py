from datetime import datetime, timezone

import pytest

from apps.accounts.models import User
from apps.catalog.models import Brand, Marketplace

HEADER = [
    "amazon-order-id", "purchase-date", "last-updated-date", "order-status", "sales-channel",
    "product-name", "sku", "asin", "item-status", "quantity", "currency", "item-price",
]


def make_report(*rows: dict) -> bytes:
    """Build a small flat-file report. Each row needs at least order id and asin."""
    lines = ["\t".join(HEADER)]
    for r in rows:
        full = {
            "purchase-date": "2026-09-01T18:00:00+00:00",
            "last-updated-date": "2026-09-01T18:00:00+00:00",
            "order-status": "Shipped",
            "sales-channel": "Amazon.com",
            "product-name": "Acme Face Serum, 1 oz",
            "sku": "SKU-1",
            "item-status": "Shipped",
            "quantity": "1",
            "currency": "USD",
            "item-price": "10.00",
            **r,
        }
        lines.append("\t".join(full.get(h, "") for h in HEADER))
    return ("\n".join(lines) + "\n").encode()


def at(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 9, day, hour, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def media_root(settings, tmp_path):
    settings.MEDIA_ROOT = tmp_path / "media"


@pytest.fixture
def us(db):
    return Marketplace.objects.get(code="US")


@pytest.fixture
def ca(db):
    return Marketplace.objects.get(code="CA")


@pytest.fixture
def acme(db, us, ca):
    brand = Brand.objects.create(name="Acme", slug="acme")
    brand.marketplaces.set([us, ca])
    return brand


@pytest.fixture
def globex(db, us):
    brand = Brand.objects.create(name="Globex", slug="globex")
    brand.marketplaces.set([us])
    return brand


@pytest.fixture
def employee(db):
    return User.objects.create_user("emp", password="pw-emp-12345", role=User.Role.EMPLOYEE)


@pytest.fixture
def client_user(db, acme):
    return User.objects.create_user("cli", password="pw-cli-12345", role=User.Role.CLIENT, brand=acme)


@pytest.fixture
def employee_client(client, employee):
    client.force_login(employee)
    return client


@pytest.fixture
def brand_client(client, client_user):
    client.force_login(client_user)
    return client
