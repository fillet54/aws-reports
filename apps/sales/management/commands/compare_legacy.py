"""
Check the new app's numbers against the old Flask app's database.

The old app kept one copy of each order (delete + re-insert on every import),
which should equal the newest version of each order here. Both sides are
grouped by UTC month and sales channel, the way the old app grouped them, and
revenue is sum(item-price) for lines that have a price, as in both apps.
"""

import json
import sqlite3
from collections import defaultdict
from datetime import timezone
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Sum
from django.db.models.functions import TruncMonth
from django.utils.text import slugify

from apps.catalog.models import Brand
from apps.sales.models import OrderLine

CENT = Decimal("0.01")


def legacy_totals(db_path: Path) -> dict[tuple[str, str], dict]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT strftime('%Y-%m', purchase_date), lower(COALESCE(sales_channel, '')),
                   COUNT(DISTINCT amazon_order_id), COALESCE(SUM(quantity), 0), SUM(item_price)
            FROM orders
            WHERE item_price IS NOT NULL AND purchase_date IS NOT NULL
            GROUP BY 1, 2
            """
        ).fetchall()
    finally:
        conn.close()
    return {
        (month, channel): {"orders": orders, "units": units, "sales": Decimal(str(round(sales, 2))).quantize(CENT)}
        for month, channel, orders, units, sales in rows
    }


def new_totals(brand: Brand) -> dict[tuple[str, str], dict]:
    rows = (
        OrderLine.objects.filter(
            brand=brand, version__is_current=True, item_price__isnull=False, purchase_date__isnull=False
        )
        .annotate(month=TruncMonth("purchase_date", tzinfo=timezone.utc))
        .values("month", "sales_channel")
        .annotate(orders=Count("amazon_order_id", distinct=True), units=Sum("quantity"), sales=Sum("item_price"))
    )
    out: dict[tuple[str, str], dict] = defaultdict(lambda: {"orders": 0, "units": 0, "sales": Decimal("0")})
    for r in rows:
        key = (r["month"].strftime("%Y-%m"), (r["sales_channel"] or "").lower())
        bucket = out[key]
        bucket["orders"] += r["orders"]
        bucket["units"] += r["units"] or 0
        bucket["sales"] += (r["sales"] or Decimal("0")).quantize(CENT)
    return dict(out)


def legacy_order_ids(db_path: Path, month: str, channel: str) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {
            r[0]
            for r in conn.execute(
                """
                SELECT DISTINCT amazon_order_id FROM orders
                WHERE item_price IS NOT NULL AND strftime('%Y-%m', purchase_date) = ?
                  AND lower(COALESCE(sales_channel, '')) = ?
                """,
                (month, channel),
            )
        }
    finally:
        conn.close()


def new_order_ids(brand: Brand, month: str, channel: str) -> set[str]:
    year, mon = map(int, month.split("-"))
    return set(
        OrderLine.objects.filter(
            brand=brand,
            version__is_current=True,
            item_price__isnull=False,
            purchase_date__year=year,
            purchase_date__month=mon,
            sales_channel__iexact=channel,
        ).values_list("amazon_order_id", flat=True)
    )


class Command(BaseCommand):
    help = "Compare monthly totals with the old Flask app's orders.sqlite for each brand."

    def add_arguments(self, parser):
        parser.add_argument("data_dir", type=Path, help="The old DATA_DIR, e.g. ~/.local/share/aws-reporting")
        parser.add_argument("--brand", help="Only this old brand id.")
        parser.add_argument("--details", action="store_true", help="List orders that differ in mismatched months.")

    def handle(self, *args, data_dir: Path, brand, details, **options):
        brands_file = data_dir / "brands.json"
        if not brands_file.exists():
            raise CommandError(f"{brands_file} not found.")

        mismatches = 0
        for item in json.loads(brands_file.read_text()):
            old_id = str(item["id"])
            if brand and old_id != brand:
                continue
            db_path = data_dir / "brands" / old_id / "orders.sqlite"
            new_brand = Brand.objects.filter(slug=slugify(old_id) or slugify(item["name"])).first()
            if not db_path.exists() or new_brand is None:
                self.stdout.write(f"{old_id}: skipped (no old database or not imported yet)")
                continue
            mismatches += self.compare(old_id, db_path, new_brand, details)

        if mismatches:
            raise CommandError(f"{mismatches} month/channel total(s) differ.")
        self.stdout.write(self.style.SUCCESS("All totals match."))

    def compare(self, old_id: str, db_path: Path, brand: Brand, details: bool) -> int:
        old, new = legacy_totals(db_path), new_totals(brand)
        empty = {"orders": 0, "units": 0, "sales": Decimal("0")}
        self.stdout.write(self.style.MIGRATE_HEADING(f"\n{brand.name} ({old_id})"))
        self.stdout.write(f"  {'month':8} {'channel':14} {'old sales':>12} {'new sales':>12} {'orders':>11} {'units':>11}")
        bad = 0
        for key in sorted(set(old) | set(new)):
            a, b = old.get(key, empty), new.get(key, empty)
            same = a == b
            bad += not same
            mark = "" if same else self.style.ERROR("  <-- differs")
            self.stdout.write(
                f"  {key[0]:8} {key[1] or '-':14} {a['sales']:>12} {b['sales']:>12} "
                f"{a['orders']:>5}/{b['orders']:<5} {a['units']:>5}/{b['units']:<5}{mark}"
            )
            if details and not same:
                old_ids, new_ids = legacy_order_ids(db_path, *key), new_order_ids(brand, *key)
                only_old, only_new = sorted(old_ids - new_ids), sorted(new_ids - old_ids)
                if only_old:
                    self.stdout.write(f"      only in old app: {', '.join(only_old[:10])}{' …' if len(only_old) > 10 else ''}")
                if only_new:
                    self.stdout.write(f"      only in new app: {', '.join(only_new[:10])}{' …' if len(only_new) > 10 else ''}")
                if not only_old and not only_new:
                    self.stdout.write("      same orders; amounts or quantities differ")
        return bad
