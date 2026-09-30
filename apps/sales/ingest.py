"""
Ingest Amazon "All Orders" flat-file reports (tab separated) into the
append-only order tables.

Every report is kept verbatim as a RawReport. Each order in it is hashed; a
new OrderVersion is stored only when the order's content differs from the
version we already knew at that time. Nothing is ever deleted or overwritten
except the is_current flag.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone as dt_timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction

from apps.catalog.models import Brand, Marketplace, Product

from .models import OrderLine, OrderVersion, RawReport

logger = logging.getLogger(__name__)

# Column order of GET_FLAT_FILE_ALL_ORDERS_DATA_BY_*_GENERAL reports.
REPORT_COLUMNS = [
    "amazon-order-id",
    "merchant-order-id",
    "purchase-date",
    "last-updated-date",
    "order-status",
    "fulfillment-channel",
    "sales-channel",
    "order-channel",
    "url",
    "ship-service-level",
    "product-name",
    "sku",
    "asin",
    "item-status",
    "quantity",
    "currency",
    "item-price",
    "item-tax",
    "shipping-price",
    "shipping-tax",
    "gift-wrap-price",
    "gift-wrap-tax",
    "item-promotion-discount",
    "ship-promotion-discount",
    "ship-city",
    "ship-state",
    "ship-postal-code",
    "ship-country",
    "promotion-ids",
    "is-business-order",
    "purchase-order-number",
    "price-designation",
]

# SQLite limits the number of query parameters; keep IN (...) lists small.
CHUNK = 500


@dataclass
class IngestResult:
    report: RawReport
    duplicate: bool = False
    # Orders seen for the first time; the rest of new_versions are changes.
    new_orders: int = 0

    @property
    def new_versions(self) -> int:
        return 0 if self.duplicate else self.report.new_versions


def decode_report(content: bytes) -> str:
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("latin-1")


def parse_rows(text: str) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if not reader.fieldnames or "amazon-order-id" not in reader.fieldnames:
        raise ValueError("Not an Amazon orders report: missing 'amazon-order-id' column.")
    rows = []
    for row in reader:
        if not (row.get("amazon-order-id") or "").strip():
            continue
        rows.append({k: (v or "").strip() for k, v in row.items() if k is not None})
    return rows


def parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dt_timezone.utc)
    return dt


def parse_decimal(value: str) -> Decimal | None:
    if value in ("", None):
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_bool(value: str) -> bool | None:
    if not value:
        return None
    return value.lower() == "true"


def order_hash(rows: list[dict[str, str]]) -> str:
    canonical = sorted(json.dumps(r, sort_keys=True) for r in rows)
    return hashlib.sha256("\n".join(canonical).encode()).hexdigest()


def extract_title(product_name: str, brand_name: str) -> str:
    """Short name from Amazon's long product name: first comma segment, minus the brand."""
    name = (product_name or "").split(",", 1)[0].strip()
    if name == "-":
        return ""
    if brand_name and name.lower().startswith(brand_name.lower()):
        name = name[len(brand_name):].lstrip(" ,:-")
    return name[:255]


def ingest_report(
    brand: Brand,
    content: bytes,
    *,
    source: str,
    fetched_at: datetime,
    report_type: str = "",
    data_start: datetime | None = None,
    data_end: datetime | None = None,
    original_filename: str = "",
) -> IngestResult:
    """Store a raw report and record any new or changed orders in it."""
    sha = hashlib.sha256(content).hexdigest()
    existing = RawReport.objects.filter(brand=brand, sha256=sha).first()
    if existing:
        return IngestResult(report=existing, duplicate=True)

    rows = parse_rows(decode_report(content))

    by_order: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_order.setdefault(row["amazon-order-id"], []).append(row)

    with transaction.atomic():
        report = RawReport(
            brand=brand,
            source=source,
            report_type=report_type,
            data_start=data_start,
            data_end=data_end,
            fetched_at=fetched_at,
            original_filename=original_filename[:255],
            sha256=sha,
            row_count=len(rows),
            order_count=len(by_order),
        )
        stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
        report.file.save(f"{brand.slug}-{stamp}-{sha[:12]}.tsv.gz", ContentFile(gzip.compress(content)), save=False)
        report.save()

        report.new_versions, new_orders = _store_versions(brand, report, by_order)
        report.save(update_fields=["new_versions"])
        _ensure_products(brand, rows)

    logger.info(
        "Ingested %s for %s: %d rows, %d orders, %d new versions",
        report.get_source_display(), brand.slug, len(rows), len(by_order), report.new_versions,
    )
    return IngestResult(report=report, new_orders=new_orders)


def _store_versions(brand: Brand, report: RawReport, by_order: dict[str, list[dict]]) -> tuple[int, int]:
    """Store changed orders. Returns (versions created, of which brand-new orders)."""
    fetched_at = report.fetched_at
    tz = ZoneInfo(settings.REPORT_TIME_ZONE)
    marketplaces = {m.sales_channel.lower(): m for m in Marketplace.objects.all()}

    order_ids = list(by_order)
    # Existing versions of these orders, oldest first.
    known: dict[str, list[OrderVersion]] = {}
    for i in range(0, len(order_ids), CHUNK):
        for v in OrderVersion.objects.filter(
            brand=brand, amazon_order_id__in=order_ids[i : i + CHUNK]
        ).order_by("fetched_at", "id"):
            known.setdefault(v.amazon_order_id, []).append(v)

    new_versions: list[OrderVersion] = []
    new_lines: list[list[dict]] = []
    demote: list[int] = []
    new_orders = 0

    for order_id, rows in by_order.items():
        digest = order_hash(rows)
        history = known.get(order_id, [])
        before = [v for v in history if v.fetched_at <= fetched_at]
        after = [v for v in history if v.fetched_at > fetched_at]

        # Unchanged since the version we already had at this time: nothing to store.
        if before and before[-1].content_hash == digest:
            continue

        if not history:
            new_orders += 1
        is_current = not after
        if is_current:
            demote.extend(v.id for v in history if v.is_current)

        last_updated = max(
            (d for d in (parse_datetime(r.get("last-updated-date", "")) for r in rows) if d),
            default=None,
        )
        new_versions.append(
            OrderVersion(
                brand=brand,
                amazon_order_id=order_id,
                report=report,
                fetched_at=fetched_at,
                last_updated_date=last_updated,
                order_status=rows[0].get("order-status", ""),
                content_hash=digest,
                is_current=is_current,
            )
        )
        new_lines.append(rows)

    for i in range(0, len(demote), CHUNK):
        OrderVersion.objects.filter(id__in=demote[i : i + CHUNK]).update(is_current=False)

    OrderVersion.objects.bulk_create(new_versions, batch_size=CHUNK)
    # bulk_create sets primary keys on SQLite and PostgreSQL.

    lines = []
    for version, rows in zip(new_versions, new_lines):
        for r in rows:
            purchased = parse_datetime(r.get("purchase-date", ""))
            channel = r.get("sales-channel", "")
            lines.append(
                OrderLine(
                    version=version,
                    brand=brand,
                    marketplace=marketplaces.get(channel.lower()),
                    amazon_order_id=version.amazon_order_id,
                    purchase_date=purchased,
                    purchase_day=purchased.astimezone(tz).date() if purchased else None,
                    order_status=r.get("order-status", ""),
                    item_status=r.get("item-status", ""),
                    fulfillment_channel=r.get("fulfillment-channel", ""),
                    sales_channel=channel,
                    asin=r.get("asin", "")[:10],
                    sku=r.get("sku", "")[:64],
                    product_name=r.get("product-name", ""),
                    quantity=parse_int(r.get("quantity")),
                    currency=r.get("currency", "")[:3],
                    item_price=parse_decimal(r.get("item-price")),
                    item_tax=parse_decimal(r.get("item-tax")),
                    shipping_price=parse_decimal(r.get("shipping-price")),
                    item_promotion_discount=parse_decimal(r.get("item-promotion-discount")),
                    ship_promotion_discount=parse_decimal(r.get("ship-promotion-discount")),
                    is_business_order=parse_bool(r.get("is-business-order", "")),
                    raw=r,
                )
            )
    OrderLine.objects.bulk_create(lines, batch_size=CHUNK)
    return len(new_versions), new_orders


def _ensure_products(brand: Brand, rows: list[dict[str, str]]) -> None:
    """Create a Product for every new ASIN and remember Amazon's latest title."""
    latest: dict[str, dict[str, str]] = {}
    for r in rows:
        asin = r.get("asin", "")
        name = r.get("product-name", "")
        if asin and name and name != "-":
            latest[asin] = r

    if not latest:
        return

    existing = {p.asin: p for p in Product.objects.filter(brand=brand, asin__in=list(latest))}
    to_create, to_update = [], []
    for asin, r in latest.items():
        amazon_title = r["product-name"][:500]
        product = existing.get(asin)
        if product is None:
            to_create.append(
                Product(
                    brand=brand,
                    asin=asin,
                    title=extract_title(amazon_title, brand.name),
                    amazon_title=amazon_title,
                    sku=r.get("sku", "")[:64],
                )
            )
        elif product.amazon_title != amazon_title:
            product.amazon_title = amazon_title
            to_update.append(product)

    Product.objects.bulk_create(to_create, ignore_conflicts=True)
    Product.objects.bulk_update(to_update, ["amazon_title"])
