"""Tools for loading real Seller Central exports and checking them against the old app."""

import json
import sqlite3
from io import StringIO

import pytest
from django.core.management import CommandError, call_command

from apps.catalog.models import Brand, Product
from apps.sales.models import OrderVersion, RawReport

from .conftest import make_report

ORDER_A = {"amazon-order-id": "111-A", "asin": "B000000001", "purchase-date": "2025-03-31T23:30:00+00:00",
           "last-updated-date": "2025-04-01T02:00:00+00:00", "order-status": "Pending", "item-price": ""}
ORDER_A_SHIPPED = {**ORDER_A, "order-status": "Shipped", "item-price": "25.00", "last-updated-date": "2025-04-02T10:00:00+00:00"}
ORDER_B = {"amazon-order-id": "702-B", "asin": "B000000002", "purchase-date": "2025-04-03T12:00:00+00:00",
           "last-updated-date": "2025-04-04T09:00:00+00:00", "sales-channel": "Amazon.ca", "currency": "CAD",
           "item-price": "40.00", "quantity": "2"}


def run(*args) -> str:
    out = StringIO()
    call_command(*args, stdout=out, stderr=out)
    return out.getvalue()


def test_folder_import_is_oldest_first_and_steps_with_limit(acme, tmp_path):
    exports = tmp_path / "raw"
    (exports / "april").mkdir(parents=True)
    # No archive timestamp in the names: dates come from last-updated-date inside.
    (exports / "april" / "later.txt").write_bytes(make_report(ORDER_A_SHIPPED, ORDER_B))
    (exports / "earlier.txt").write_bytes(make_report(ORDER_A))
    (exports / "notes.md").write_text("not a report")

    first = run("import_report", "acme", str(exports), "--limit", "1")
    assert "earlier.txt" in first and "later.txt" not in first
    assert "1 new, 0 changed" in first and "not checked yet" in first

    second = run("import_report", "acme", str(exports), "--limit", "1")
    assert "later.txt" in second
    assert "1 new, 1 changed, 0 unchanged" in second

    third = run("import_report", "acme", str(exports))
    assert "imported 0, already imported 2" in third

    reports = RawReport.objects.order_by("fetched_at")
    assert [r.original_filename for r in reports] == ["earlier.txt", "later.txt"]
    assert reports[1].fetched_at.isoformat() == "2025-04-04T09:00:00+00:00"
    assert OrderVersion.objects.get(amazon_order_id="111-A", is_current=True).order_status == "Shipped"


def test_bad_file_is_reported_and_others_still_import(acme, tmp_path):
    (tmp_path / "bad.txt").write_text("not\ta\treport\n")
    (tmp_path / "good.txt").write_bytes(make_report(ORDER_B))
    output = run("import_report", "acme", str(tmp_path))
    assert "missing 'amazon-order-id'" in output
    assert "imported 1" in output and "failed 1" in output


def make_legacy_dir(tmp_path, orders):
    data = tmp_path / "old"
    brand_dir = data / "brands" / "acme"
    (brand_dir / "archive").mkdir(parents=True)
    (data / "brands.json").write_text(json.dumps([{"id": "acme", "name": "Acme"}]))
    (brand_dir / "archive" / "20250401T120000Z__week1.txt").write_bytes(make_report(ORDER_A))
    (brand_dir / "archive" / "20250405T120000Z__week2.txt").write_bytes(make_report(ORDER_A_SHIPPED, ORDER_B))

    conn = sqlite3.connect(brand_dir / "orders.sqlite")
    conn.execute("CREATE TABLE orders (amazon_order_id TEXT, purchase_date TEXT, sales_channel TEXT, quantity INTEGER, item_price REAL)")
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders)
    conn.execute("CREATE TABLE asin_meta (asin TEXT PRIMARY KEY, title_override TEXT, brand TEXT, category TEXT, subcategory TEXT, cost REAL, launch_date TEXT, notes TEXT)")
    conn.execute("INSERT INTO asin_meta VALUES ('B000000001', 'Hero Serum', NULL, 'Skin', NULL, 4.5, NULL, NULL)")
    conn.commit()
    conn.close()
    return data


# What the old app's database holds after importing the same two files.
LEGACY_ORDERS = [
    ("111-A", "2025-03-31 23:30:00", "Amazon.com", 1, 25.0),
    ("702-B", "2025-04-03 12:00:00", "Amazon.ca", 2, 40.0),
]


def test_import_legacy_then_compare_matches(db, us, ca, tmp_path):
    data = make_legacy_dir(tmp_path, LEGACY_ORDERS)
    run("import_legacy", str(data))

    brand = Brand.objects.get(slug="acme")
    assert Product.objects.get(brand=brand, asin="B000000001").title == "Hero Serum"
    assert RawReport.objects.filter(brand=brand, source="legacy").count() == 2

    output = run("compare_legacy", str(data))
    assert "All totals match." in output
    # Grouped in UTC like the old app: the Mar 31 23:30 UTC order stays in March.
    assert "2025-03" in output


def test_compare_legacy_reports_differences(db, us, ca, tmp_path):
    data = make_legacy_dir(tmp_path, LEGACY_ORDERS + [("111-C", "2025-04-10 10:00:00", "Amazon.com", 1, 9.99)])
    run("import_legacy", str(data))

    out = StringIO()
    with pytest.raises(CommandError, match="1 month/channel"):
        call_command("compare_legacy", str(data), "--details", stdout=out)
    assert "only in old app: 111-C" in out.getvalue()
