from datetime import timedelta

from django.urls import reverse
from django.utils import timezone

from apps.catalog.models import Brand, Product
from apps.reports.models import SharedReport
from apps.reports.periods import report_today
from apps.sales.amazon import REPORT_BY_ORDER_DATE, DummyAmazonClient
from apps.sales.ingest import ingest_report
from apps.sales.models import RawReport
from apps.sales.sync import sync_brand

from .conftest import make_report


def test_employee_creates_brand_with_encrypted_token(employee_client, us, ca):
    response = employee_client.post(reverse("catalog:brand_create"), {
        "name": "Initech", "slug": "initech", "marketplaces": [us.pk, ca.pk],
        "selling_partner_id": "A1B2C3", "refresh_token": "Atzr|secret", "sync_enabled": "on",
    })
    assert response.status_code == 302
    brand = Brand.objects.get(slug="initech")
    assert set(brand.marketplaces.values_list("code", flat=True)) == {"US", "CA"}
    assert "secret" not in brand.refresh_token_encrypted
    assert brand.get_refresh_token() == "Atzr|secret"

    # Saving without a token keeps the stored one.
    employee_client.post(reverse("catalog:brand_edit", args=["initech"]), {
        "name": "Initech Inc", "slug": "initech", "marketplaces": [us.pk], "selling_partner_id": "A1B2C3",
    })
    brand.refresh_from_db()
    assert brand.name == "Initech Inc" and brand.get_refresh_token() == "Atzr|secret"


def test_employee_adds_and_renames_product_by_asin(employee_client, acme):
    employee_client.post(reverse("catalog:product_create", args=["acme"]), {"asin": "B0ABCDEFGH", "title": "Serum"})
    assert Product.objects.get(brand=acme, asin="B0ABCDEFGH").title == "Serum"

    url = reverse("catalog:product_edit", args=["acme", "B0ABCDEFGH"])
    employee_client.post(url, {"asin": "IGNORED", "title": "Hero Serum"})
    product = Product.objects.get(brand=acme, asin="B0ABCDEFGH")
    assert product.title == "Hero Serum"


def test_upload_report(employee_client, acme):
    from django.core.files.uploadedfile import SimpleUploadedFile

    upload = SimpleUploadedFile("orders.txt", make_report({"amazon-order-id": "111-1", "asin": "B000000001"}))
    employee_client.post(reverse("catalog:brand_upload", args=["acme"]), {"report_file": upload})
    assert RawReport.objects.filter(brand=acme, source="upload").count() == 1


def test_shared_link_is_frozen_until_refreshed(employee_client, acme):
    now = timezone.now()
    day = report_today()
    purchase = (now - timedelta(hours=1)).isoformat()

    ingest_report(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "purchase-date": purchase, "item-price": "10.00"}),
                  source="upload", fetched_at=now - timedelta(minutes=30))

    response = employee_client.post(reverse("reports:share_create"), {"brand": "acme", "period": "month", "start": day.isoformat()})
    shared = SharedReport.objects.get()
    assert response["Location"] == shared.get_absolute_url()

    first = employee_client.get(shared.get_absolute_url())
    assert first.context["summary"]["current"]["orders"] == 1

    # New data arrives after the link was made.
    ingest_report(acme, make_report({"amazon-order-id": "111-2", "asin": "B000000001", "purchase-date": purchase, "item-price": "50.00"}),
                  source="upload", fetched_at=timezone.now() + timedelta(seconds=1))
    assert employee_client.get(shared.get_absolute_url()).context["summary"]["current"]["orders"] == 1

    shared.data_cutoff = timezone.now() + timedelta(seconds=2)  # refresh, avoiding same-instant ties
    shared.save()
    assert employee_client.get(shared.get_absolute_url()).context["summary"]["current"]["orders"] == 2

    assert employee_client.post(reverse("reports:shared_refresh", args=[shared.token])).status_code == 302


def test_dummy_client_is_stable_and_sync_only_stores_changes(acme):
    now = timezone.now()
    client = DummyAmazonClient(now=now)
    a = client.fetch_orders_report(acme, now - timedelta(days=5), now, REPORT_BY_ORDER_DATE)
    b = client.fetch_orders_report(acme, now - timedelta(days=5), now, REPORT_BY_ORDER_DATE)
    assert a == b and a.count(b"\n") > 1

    first = sync_brand(acme, DummyAmazonClient(now=now), now=now, since=now - timedelta(days=3))
    again = sync_brand(acme, DummyAmazonClient(now=now), now=now + timedelta(seconds=1), since=now - timedelta(days=3))
    assert first.new_versions > 0
    assert again.new_versions == 0
    assert Product.objects.filter(brand=acme).exists()
