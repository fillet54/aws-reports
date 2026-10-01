"""Loading a folder of saved Seller Central exports with `import_report`."""

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
    # Dates come from last-updated-date inside each file, not from the names.
    (exports / "april" / "a-later.txt").write_bytes(make_report(ORDER_A_SHIPPED, ORDER_B))
    (exports / "z-earlier.txt").write_bytes(make_report(ORDER_A))
    (exports / "notes.md").write_text("not a report")

    first = run("import_report", "acme", str(exports), "--limit", "1")
    assert "z-earlier.txt" in first and "a-later.txt" not in first
    assert "1 new, 0 changed" in first and "not checked yet" in first

    second = run("import_report", "acme", str(exports), "--limit", "1")
    assert "a-later.txt" in second
    assert "1 new, 1 changed, 0 unchanged" in second

    third = run("import_report", "acme", str(exports))
    assert "imported 0, already imported 2" in third

    reports = RawReport.objects.order_by("fetched_at")
    assert [r.original_filename for r in reports] == ["z-earlier.txt", "a-later.txt"]
    assert reports[1].fetched_at.isoformat() == "2025-04-04T09:00:00+00:00"
    assert OrderVersion.objects.get(amazon_order_id="111-A", is_current=True).order_status == "Shipped"


def test_timestamp_in_file_name_wins(acme, tmp_path):
    (tmp_path / "20250410T080000Z__orders.txt").write_bytes(make_report(ORDER_B))
    output = run("import_report", "acme", str(tmp_path))
    assert "from file name" in output
    assert RawReport.objects.get().fetched_at.isoformat() == "2025-04-10T08:00:00+00:00"


def test_comma_separated_csv_is_accepted(acme, tmp_path):
    tsv = make_report(ORDER_B).decode()
    (tmp_path / "orders.csv").write_text(tsv.replace("\t", ","))
    run("import_report", "acme", str(tmp_path))
    assert OrderVersion.objects.filter(amazon_order_id="702-B").exists()


def test_create_brand_on_first_load(db, us, ca, tmp_path):
    (tmp_path / "orders.txt").write_bytes(make_report(ORDER_B))
    with pytest.raises(CommandError, match="--create"):
        run("import_report", "initech", str(tmp_path))

    output = run("import_report", "initech", str(tmp_path), "--create", "Initech")
    assert "Created brand Initech" in output
    brand = Brand.objects.get(slug="initech")
    assert set(brand.marketplaces.values_list("code", flat=True)) == {"US", "CA"}
    assert Product.objects.filter(brand=brand, asin="B000000002").exists()


def test_bad_file_is_reported_and_others_still_import(acme, tmp_path):
    (tmp_path / "bad.txt").write_text("not\ta\treport\n")
    (tmp_path / "good.txt").write_bytes(make_report(ORDER_B))
    output = run("import_report", "acme", str(tmp_path))
    assert "missing 'amazon-order-id'" in output
    assert "imported 1" in output and "failed 1" in output
