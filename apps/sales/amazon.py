"""
Clients that fetch Amazon "All Orders" reports for a brand.

Both clients return the raw flat-file report bytes; ingest.py doesn't care
where they came from.

    DummyAmazonClient  deterministic fake data for development and demos
    SpApiClient        the real Selling Partner API (python-amazon-sp-api)
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import random
import time
from datetime import date, datetime, timedelta, timezone as dt_timezone
from decimal import Decimal
from typing import Protocol

from django.conf import settings
from django.utils import timezone

from apps.catalog.models import Brand

from .ingest import REPORT_COLUMNS

logger = logging.getLogger(__name__)

REPORT_BY_LAST_UPDATE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"
REPORT_BY_ORDER_DATE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"


class AmazonClient(Protocol):
    name: str

    def fetch_orders_report(
        self, brand: Brand, start: datetime, end: datetime, report_type: str
    ) -> bytes: ...


def get_client(name: str | None = None) -> AmazonClient:
    name = name or settings.AMAZON_CLIENT
    if name == "dummy":
        return DummyAmazonClient()
    if name == "sp_api":
        return SpApiClient()
    raise ValueError(f"Unknown AMAZON_CLIENT {name!r} (expected 'dummy' or 'sp_api').")


# ---------------------------------------------------------------------------
# Real SP-API client
# ---------------------------------------------------------------------------


class SpApiError(RuntimeError):
    pass


class SpApiClient:
    """
    Requests a report, waits for Amazon to build it, and downloads it.

    Needs SP_API_LWA_APP_ID / SP_API_LWA_CLIENT_SECRET in the environment and a
    refresh token stored on the brand. Not exercised against Amazon yet.
    """

    name = "sp_api"
    poll_seconds = 30
    timeout_seconds = 30 * 60

    def fetch_orders_report(self, brand, start, end, report_type):
        from sp_api.api import Reports
        from sp_api.base import Marketplaces

        refresh_token = brand.get_refresh_token()
        if not refresh_token:
            raise SpApiError(f"Brand {brand.slug!r} has no SP-API refresh token.")
        if not (settings.SP_API_LWA_APP_ID and settings.SP_API_LWA_CLIENT_SECRET):
            raise SpApiError("SP_API_LWA_APP_ID and SP_API_LWA_CLIENT_SECRET must be set.")

        credentials = {
            "refresh_token": refresh_token,
            "lwa_app_id": settings.SP_API_LWA_APP_ID,
            "lwa_client_secret": settings.SP_API_LWA_CLIENT_SECRET,
        }
        marketplace_ids = [m.amazon_marketplace_id for m in brand.marketplaces.all()]
        if not marketplace_ids:
            raise SpApiError(f"Brand {brand.slug!r} has no marketplaces selected.")

        # US and Canada are both served by the North America endpoint.
        api = Reports(marketplace=Marketplaces.US, credentials=credentials)
        created = api.create_report(
            reportType=report_type,
            dataStartTime=start.isoformat(),
            dataEndTime=end.isoformat(),
            marketplaceIds=marketplace_ids,
        )
        report_id = created.payload["reportId"]

        deadline = time.monotonic() + self.timeout_seconds
        while True:
            report = api.get_report(report_id).payload
            status = report.get("processingStatus")
            if status == "DONE":
                break
            if status == "CANCELLED":
                # Amazon cancels reports that would be empty.
                return "\t".join(REPORT_COLUMNS).encode() + b"\n"
            if status == "FATAL":
                raise SpApiError(f"Amazon failed to build report {report_id}.")
            if time.monotonic() > deadline:
                raise SpApiError(f"Timed out waiting for report {report_id}.")
            time.sleep(self.poll_seconds)

        document = api.get_report_document(report["reportDocumentId"], download=True)
        return document.payload["document"].encode("utf-8")


# ---------------------------------------------------------------------------
# Dummy client
# ---------------------------------------------------------------------------

_PRODUCT_WORDS = [
    ("Hydrating Face Serum", Decimal("28.99")),
    ("Vitamin C Brightening Cream", Decimal("34.50")),
    ("Gentle Foaming Cleanser", Decimal("16.99")),
    ("Overnight Repair Mask", Decimal("24.00")),
    ("Mineral Sunscreen SPF 50", Decimal("19.95")),
    ("Rosewater Toner", Decimal("14.49")),
    ("Retinol Night Oil", Decimal("39.00")),
    ("Lip Repair Balm 3-Pack", Decimal("12.99")),
    ("Exfoliating Body Scrub", Decimal("21.50")),
    ("Under-Eye Gel Patches", Decimal("17.25")),
]

_CHANNELS = {
    "US": ("Amazon.com", "USD", "US", 1.0),
    "CA": ("Amazon.ca", "CAD", "CA", 0.18),
}


def _seed(*parts) -> int:
    return int(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:16], 16)


class DummyAmazonClient:
    """
    Generates a stable, realistic order history per brand.

    The same order always has the same id, items and prices; only its status
    changes as time passes (Pending -> Unshipped -> Shipped, some Cancelled),
    so repeated syncs exercise the "store a new version only when it changes"
    path. `now` can be pinned to simulate fetching at a past moment.
    """

    name = "dummy"

    def __init__(self, now: datetime | None = None, orders_per_day: float = 14.0):
        self.now = now
        self.orders_per_day = orders_per_day

    def catalog(self, brand: Brand) -> list[tuple[str, str, str, Decimal]]:
        rng = random.Random(_seed("catalog", brand.slug))
        picks = rng.sample(_PRODUCT_WORDS, 6)
        items = []
        for i, (name, price) in enumerate(picks):
            asin = "B0" + "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(8))
            sku = f"{brand.slug[:6].upper()}-{i + 1:03d}"
            title = f"{brand.name} {name}, Clean Beauty, {rng.choice(['1 fl oz', '1.7 oz', '50 ml', '3.4 oz'])}"
            items.append((asin, sku, title, price))
        return items

    def fetch_orders_report(self, brand, start, end, report_type):
        now = min(self.now or timezone.now(), end)
        codes = {m.code for m in brand.marketplaces.all()} or {"US"}
        catalog = self.catalog(brand)

        # Orders change status for up to ~3 days after purchase, so a
        # last-update window also contains orders bought a few days earlier.
        lookback = timedelta(days=4) if report_type == REPORT_BY_LAST_UPDATE else timedelta(0)
        first_day = (start - lookback).date()
        last_day = now.date()

        rows = []
        day = first_day
        while day <= last_day:
            for code in sorted(codes & set(_CHANNELS)):
                for order in self._orders_for_day(brand, catalog, day, code):
                    if order["purchased"] > now:
                        continue
                    status, updated = self._status(order, now)
                    in_window = (
                        start <= updated <= end
                        if report_type == REPORT_BY_LAST_UPDATE
                        else start <= order["purchased"] <= end
                    )
                    if in_window:
                        rows.extend(self._rows(order, status, updated))
            day += timedelta(days=1)

        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=REPORT_COLUMNS, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        return out.getvalue().encode("utf-8")

    def _orders_for_day(self, brand, catalog, day: date, code: str):
        channel, currency, country, share = _CHANNELS[code]
        rng = random.Random(_seed("day", brand.slug, day.isoformat(), code))
        # Year-over-year growth, a weekly rhythm and a Q4 bump.
        growth = 1.0 + 0.25 * ((day - date(2024, 1, 1)).days / 365)
        weekday = [1.0, 0.95, 0.95, 1.0, 1.1, 1.25, 1.2][day.weekday()]
        season = 1.5 if day.month in (11, 12) else 0.85 if day.month in (1, 2) else 1.0
        brand_scale = 0.6 + (_seed("scale", brand.slug) % 100) / 100
        expected = self.orders_per_day * share * growth * weekday * season * brand_scale
        count = max(0, int(rng.gauss(expected, expected ** 0.5)))

        fx = Decimal("1.36") if currency == "CAD" else Decimal("1")
        for n in range(count):
            seconds = rng.randint(0, 86399)
            purchased = datetime(day.year, day.month, day.day, tzinfo=dt_timezone.utc) + timedelta(seconds=seconds)
            order_id = f"{rng.randint(111, 114)}-{rng.randint(1000000, 9999999)}-{rng.randint(1000000, 9999999)}"
            lines = []
            for asin, sku, title, price in rng.sample(catalog, 1 if rng.random() < 0.85 else 2):
                qty = 1 if rng.random() < 0.8 else rng.randint(2, 3)
                unit = (price * fx).quantize(Decimal("0.01"))
                promo = (unit * qty * Decimal("0.15")).quantize(Decimal("0.01")) if rng.random() < 0.1 else Decimal("0")
                lines.append({"asin": asin, "sku": sku, "title": title, "qty": qty, "unit": unit, "promo": promo})
            yield {
                "id": order_id,
                "purchased": purchased,
                "channel": channel,
                "currency": currency,
                "country": country,
                "fba": rng.random() < 0.9,
                "cancel": rng.random() < 0.03,
                "business": rng.random() < 0.05,
                "lines": lines,
                "ship_hours": rng.randint(6, 40),
            }

    @staticmethod
    def _status(order, now: datetime) -> tuple[str, datetime]:
        purchased = order["purchased"]
        age = now - purchased
        if age < timedelta(minutes=30):
            return "Pending", purchased
        if order["cancel"] and age >= timedelta(hours=2):
            return "Cancelled", purchased + timedelta(hours=2)
        shipped_at = purchased + timedelta(hours=order["ship_hours"])
        if now < shipped_at:
            return "Unshipped" if order["fba"] is False else "Pending", purchased + timedelta(minutes=30)
        return "Shipped", shipped_at

    @staticmethod
    def _rows(order, status: str, updated: datetime) -> list[dict]:
        rows = []
        for line in order["lines"]:
            priced = status in ("Shipped", "Unshipped")
            total = line["unit"] * line["qty"]
            rows.append({
                "amazon-order-id": order["id"],
                "merchant-order-id": "",
                "purchase-date": order["purchased"].strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                "last-updated-date": updated.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                "order-status": status,
                "fulfillment-channel": "Amazon" if order["fba"] else "Merchant",
                "sales-channel": order["channel"],
                "order-channel": "",
                "url": "",
                "ship-service-level": "Expedited" if order["fba"] else "Standard",
                "product-name": line["title"],
                "sku": line["sku"],
                "asin": line["asin"],
                "item-status": "Cancelled" if status == "Cancelled" else status,
                "quantity": "0" if status == "Cancelled" else str(line["qty"]),
                "currency": order["currency"] if priced else "",
                "item-price": f"{total:.2f}" if priced else "",
                "item-tax": f"{(total * Decimal('0.07')).quantize(Decimal('0.01')):.2f}" if priced else "",
                "shipping-price": "0.00" if priced else "",
                "shipping-tax": "0.00" if priced else "",
                "gift-wrap-price": "",
                "gift-wrap-tax": "",
                "item-promotion-discount": f"{-line['promo']:.2f}" if priced and line["promo"] else "",
                "ship-promotion-discount": "",
                "ship-city": "",
                "ship-state": "",
                "ship-postal-code": "",
                "ship-country": order["country"],
                "promotion-ids": "",
                "is-business-order": "true" if order["business"] else "false",
                "purchase-order-number": "",
                "price-designation": "",
            })
        return rows
