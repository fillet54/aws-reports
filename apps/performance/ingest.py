"""
Store raw performance responses and turn them into daily rows.

The raw response is always kept (DataPull). Daily rows for the days a
response covers are replaced, because Amazon revises recent days.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal

from django.core.files.base import ContentFile
from django.db import transaction

from apps.catalog.models import Brand, Marketplace

from .models import DailyAds, DailySubscriptions, DailyTraffic, DataPull

CENT = Decimal("0.01")


def _money(value) -> Decimal:
    if isinstance(value, dict):  # {"amount": 12.3, "currencyCode": "USD"}
        value = value.get("amount")
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value)).quantize(CENT)


def _int(value) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _optional_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value)).quantize(CENT)


def parse_date(value) -> date:
    text = str(value)[:10]
    if len(text) == 8 and text.isdigit():  # 20260901
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text)


def save_pull(brand, marketplace, kind, source, start, end, payload, fetched_at, days) -> DataPull:
    pull = DataPull(
        brand=brand, marketplace=marketplace, kind=kind, source=source,
        start_date=start, end_date=end, fetched_at=fetched_at, days=days,
    )
    raw = gzip.compress(json.dumps(payload, default=str).encode())
    pull.file.save(f"{brand.slug}-{marketplace.code}-{kind}-{start}-{end}.json.gz", ContentFile(raw), save=False)
    pull.save()
    return pull


# ---------------------------------------------------------------------------
# Parsers: raw Amazon shapes -> {date: fields}
# ---------------------------------------------------------------------------


def parse_traffic(payload: dict) -> dict[date, dict]:
    out = {}
    for row in payload.get("salesAndTrafficByDate") or []:
        sales, traffic = row.get("salesByDate") or {}, row.get("trafficByDate") or {}
        out[parse_date(row["date"])] = {
            "ordered_product_sales": _money(sales.get("orderedProductSales")),
            "units_ordered": _int(sales.get("unitsOrdered")),
            "sessions": _int(traffic.get("sessions")),
            "page_views": _int(traffic.get("pageViews")),
            "average_offer_count": _optional_decimal(traffic.get("averageOfferCount")),
            "buy_box_percentage": _optional_decimal(traffic.get("buyBoxPercentage")),
        }
    return out


def parse_subscriptions(payload: dict) -> dict[date, dict]:
    out = {}
    for row in payload.get("metrics") or []:
        day = parse_date(row["timeInterval"]["startDate"])
        out[day] = {
            "active_subscriptions": _int(row.get("activeSubscriptions")),
            "shipped_units": _int(row.get("shippedSubscriptionUnits")),
            "revenue": _money(row.get("totalSubscriptionsRevenue")),
        }
    return out


# Column names differ by ad product in the v3 reports.
AD_COLUMNS = {
    "SP": {"sales": "sales7d", "orders": "purchases7d", "units": "unitsSoldClicks7d"},
    "SB": {"sales": "sales", "orders": "purchases", "units": "unitsSold"},
    "SD": {"sales": "sales", "orders": "purchases", "units": "unitsSold"},
    "DSP": {"sales": "totalSales", "orders": "purchases", "units": "unitsSold", "cost": "totalCost"},
}


def parse_ad_rows(product: str, rows: list[dict]) -> dict[date, dict]:
    """Sum per-campaign (or per-order, for DSP) rows into one row per day."""
    columns = AD_COLUMNS[product]
    cost_key = columns.get("cost", "cost")
    out: dict[date, dict] = defaultdict(
        lambda: {"cost": Decimal("0"), "sales": Decimal("0"), "impressions": 0, "clicks": 0, "orders": 0, "units": 0}
    )
    for row in rows or []:
        day = out[parse_date(row["date"])]
        day["cost"] += _money(row.get(cost_key))
        day["sales"] += _money(row.get(columns["sales"]))
        day["impressions"] += _int(row.get("impressions"))
        day["clicks"] += _int(row.get("clicks") or row.get("clickThroughs"))
        day["orders"] += _int(row.get(columns["orders"]))
        day["units"] += _int(row.get(columns["units"]))
    return dict(out)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def _replace_days(model, brand, marketplace, start, end, by_day, pull, **extra):
    """Replace stored rows for start..end with the response's days."""
    filters = {"brand": brand, "marketplace": marketplace, "date__gte": start, "date__lte": end, **extra}
    model.objects.filter(**filters).delete()
    model.objects.bulk_create([
        model(brand=brand, marketplace=marketplace, date=day, pull=pull, **extra, **fields)
        for day, fields in sorted(by_day.items())
        if start <= day <= end
    ])
    return len(by_day)


def store_traffic(brand: Brand, marketplace: Marketplace, start, end, payload, *, source, fetched_at) -> int:
    by_day = parse_traffic(payload)
    with transaction.atomic():
        pull = save_pull(brand, marketplace, DataPull.Kind.TRAFFIC, source, start, end, payload, fetched_at, len(by_day))
        return _replace_days(DailyTraffic, brand, marketplace, start, end, by_day, pull)


def store_subscriptions(brand, marketplace, start, end, payload, *, source, fetched_at) -> int:
    by_day = parse_subscriptions(payload)
    with transaction.atomic():
        pull = save_pull(brand, marketplace, DataPull.Kind.SUBSCRIPTIONS, source, start, end, payload, fetched_at, len(by_day))
        return _replace_days(DailySubscriptions, brand, marketplace, start, end, by_day, pull)


def store_sponsored_ads(brand, marketplace, start, end, payload: dict, *, source, fetched_at) -> int:
    with transaction.atomic():
        pull = save_pull(brand, marketplace, DataPull.Kind.ADS, source, start, end, payload, fetched_at, 0)
        days = 0
        ranges = payload.get("ranges", {})
        for product in DailyAds.SEARCH:
            if product not in payload:
                continue
            product_start, product_end = start, end
            if product in ranges:
                product_start, product_end = (parse_date(d) for d in ranges[product])
            by_day = parse_ad_rows(product, payload[product])
            days += _replace_days(
                DailyAds, brand, marketplace, product_start, product_end, by_day, pull, ad_product=product
            )
        pull.days = days
        pull.save(update_fields=["days"])
        return days


def store_dsp(brand, marketplace, start, end, rows: list, *, source, fetched_at) -> int:
    by_day = parse_ad_rows("DSP", rows)
    with transaction.atomic():
        pull = save_pull(brand, marketplace, DataPull.Kind.DSP, source, start, end, rows, fetched_at, len(by_day))
        return _replace_days(DailyAds, brand, marketplace, start, end, by_day, pull, ad_product="DSP")
