from datetime import date
from decimal import Decimal

from apps.catalog.models import Product
from apps.sales import metrics
from apps.sales.ingest import extract_title, ingest_report
from apps.sales.metrics import Scope
from apps.sales.models import OrderLine, OrderVersion, RawReport

from .conftest import at, make_report

UPLOAD = RawReport.Source.UPLOAD


def ingest(brand, content, when):
    return ingest_report(brand, content, source=UPLOAD, fetched_at=when)


def test_unchanged_orders_are_not_stored_twice(acme):
    first = make_report({"amazon-order-id": "111-1", "asin": "B000000001"})
    ingest(acme, first, at(1))
    # Same order, different report file (extra order) fetched later.
    result = ingest(acme, make_report(
        {"amazon-order-id": "111-1", "asin": "B000000001"},
        {"amazon-order-id": "111-2", "asin": "B000000001"},
    ), at(2))

    assert result.new_versions == 1
    assert OrderVersion.objects.filter(amazon_order_id="111-1").count() == 1


def test_identical_file_is_skipped(acme):
    content = make_report({"amazon-order-id": "111-1", "asin": "B000000001"})
    ingest(acme, content, at(1))
    assert ingest(acme, content, at(2)).duplicate
    assert RawReport.objects.count() == 1


def test_changed_order_gets_new_current_version_and_keeps_history(acme):
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "order-status": "Pending", "item-price": ""}), at(1))
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "order-status": "Shipped"}), at(2))

    versions = OrderVersion.objects.filter(amazon_order_id="111-1").order_by("fetched_at")
    assert [v.order_status for v in versions] == ["Pending", "Shipped"]
    assert [v.is_current for v in versions] == [False, True]
    assert OrderLine.objects.count() == 2  # history kept


def test_out_of_order_ingest_does_not_replace_newer_data(acme):
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "20.00"}), at(5))
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "15.00"}), at(3))

    current = OrderVersion.objects.get(is_current=True)
    assert current.fetched_at == at(5)
    assert OrderVersion.objects.count() == 2


def test_as_of_query_sees_data_known_at_that_time(acme):
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "10.00"}), at(2))
    ingest(acme, make_report(
        {"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "12.00"},
        {"amazon-order-id": "111-2", "asin": "B000000001", "item-price": "30.00"},
    ), at(4))

    day = date(2026, 9, 1)
    live = metrics.totals(Scope(brands=(acme,)), day, day)
    frozen = metrics.totals(Scope(brands=(acme,), as_of=at(3)), day, day)

    assert live["sales"] == Decimal("42.00") and live["orders"] == 2
    assert frozen["sales"] == Decimal("10.00") and frozen["orders"] == 1


def test_products_created_from_orders_with_short_title(acme):
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "product-name": "Acme Face Serum, 1 oz"}), at(1))
    product = Product.objects.get(brand=acme, asin="B000000001")
    assert product.title == "Face Serum"
    assert product.amazon_title == "Acme Face Serum, 1 oz"

    # Edited titles survive later imports.
    product.title = "Serum (hero)"
    product.save()
    ingest(acme, make_report({"amazon-order-id": "111-2", "asin": "B000000001", "product-name": "Acme Face Serum XL, 2 oz"}), at(2))
    product.refresh_from_db()
    assert product.title == "Serum (hero)"
    assert product.amazon_title == "Acme Face Serum XL, 2 oz"


def test_extract_title():
    assert extract_title("Acme Face Serum, 1 oz", "Acme") == "Face Serum"
    assert extract_title("-", "Acme") == ""


def test_purchase_day_uses_report_time_zone(acme, settings):
    settings.REPORT_TIME_ZONE = "America/Los_Angeles"
    # 03:00 UTC on Sep 2 is still Sep 1 in California.
    ingest(acme, make_report({"amazon-order-id": "111-1", "asin": "B000000001", "purchase-date": "2026-09-02T03:00:00+00:00"}), at(3))
    assert OrderLine.objects.get().purchase_day == date(2026, 9, 1)


def test_combining_stores_converts_to_usd(acme, ca):
    ca.usd_rate = Decimal("0.75")
    ca.save()
    ingest(acme, make_report(
        {"amazon-order-id": "111-1", "asin": "B000000001", "item-price": "10.00"},
        {"amazon-order-id": "702-1", "asin": "B000000001", "item-price": "20.00", "sales-channel": "Amazon.ca", "currency": "CAD"},
    ), at(2))
    day = date(2026, 9, 1)
    assert metrics.totals(Scope(brands=(acme,)), day, day)["sales"] == Decimal("25.00")
    assert metrics.totals(Scope(brands=(acme,), marketplace=ca), day, day)["sales"] == Decimal("20.00")
