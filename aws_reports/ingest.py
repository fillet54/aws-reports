
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import shutil
import hashlib


from .config import BRAND_PATH, BRAND_ARCHIVE_PATH, BRANDS_FILE 


def normalize_date(dt_str):
    if not dt_str:
        return None
    # Adjust if your Real Amazon timestamps differ
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def to_int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def apply_report_to_db(conn, csv_path: Path) -> int:
    """
    Read csv_path, delete existing orders with those amazon_order_ids,
    and insert fresh rows. Returns row_count.
    """
    cur = conn.cursor()

    order_ids = set()
    rows = []

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")

        for r in reader:
            oid = r["amazon-order-id"]
            order_ids.add(oid)

            rows.append((
                oid,
                r.get("merchant-order-id"),
                normalize_date(r.get("purchase-date")),
                normalize_date(r.get("last-updated-date")),
                r.get("order-status"),
                r.get("fulfillment-channel"),
                r.get("sales-channel"),
                r.get("order-channel"),
                r.get("url"),
                r.get("ship-service-level"),
                r.get("product-name"),
                r.get("sku"),
                r.get("asin"),
                r.get("item-status"),
                to_int(r.get("quantity")),
                r.get("currency"),
                to_float(r.get("item-price")),
                to_float(r.get("item-tax")),
                to_float(r.get("shipping-price")),
                to_float(r.get("shipping-tax")),
                to_float(r.get("gift-wrap-price")),
                to_float(r.get("gift-wrap-tax")),
                to_float(r.get("item-promotion-discount")),
                to_float(r.get("ship-promotion-discount")),
                r.get("ship-city"),
                r.get("ship-state"),
                r.get("ship-postal-code"),
                r.get("ship-country"),
                r.get("promotion-ids"),
                1 if r.get("is-business-order") == "true" else 0 if r.get("is-business-order") else None,
                r.get("purchase-order-number"),
                r.get("price-designation"),
                r.get("buyer-identification-number"),
                r.get("buyer-identification-type"),
            ))

    if not rows:
        return 0

    with conn:  # transaction
        placeholders = ",".join(["?"] * len(order_ids))
        cur.execute(
            f"DELETE FROM orders WHERE amazon_order_id IN ({placeholders})",
            list(order_ids),
        )

        cur.executemany("""
            INSERT INTO orders (
                amazon_order_id,
                merchant_order_id,
                purchase_date,
                last_updated_date,
                order_status,
                fulfillment_channel,
                sales_channel,
                order_channel,
                url,
                ship_service_level,
                product_name,
                sku,
                asin,
                item_status,
                quantity,
                currency,
                item_price,
                item_tax,
                shipping_price,
                shipping_tax,
                gift_wrap_price,
                gift_wrap_tax,
                item_promotion_discount,
                ship_promotion_discount,
                ship_city,
                ship_state,
                ship_postal_code,
                ship_country,
                promotion_ids,
                is_business_order,
                purchase_order_number,
                price_designation,
                buyer_identification_number,
                buyer_identification_type
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );
        """, rows)

    return len(rows)


def _extract_name(product_name: str) -> str | None:
    if not product_name:
        return None
    name = product_name.split(",", 1)[0].strip()
    if name == "-":
        return None
    return name or None


def _strip_brand_prefix(name: str | None, brand_name: str | None) -> str | None:
    if not name or not brand_name:
        return name
    brand_clean = brand_name.strip()
    if not brand_clean:
        return name
    name_lower = name.lower()
    brand_lower = brand_clean.lower()
    if name_lower.startswith(brand_lower):
        stripped = name[len(brand_clean):].lstrip(" ,:-")
        return stripped or None
    return name


def _get_brand_name(brand_id: str) -> str | None:
    if not BRANDS_FILE.exists():
        return None
    try:
        with BRANDS_FILE.open("r", encoding="utf-8") as f:
            brands = json.load(f)
        for b in brands:
            if isinstance(b, dict) and str(b.get("id")) == str(brand_id):
                return b.get("name")
    except Exception:
        return None
    return None


def ensure_asin_meta(conn, brand_id: str) -> None:
    """
    Create asin_meta rows for any ASINs seen in orders that do not already
    have metadata. Uses the first segment of product_name (split on ",") as
    the title_override.
    """
    brand_name = _get_brand_name(brand_id)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT o.asin, MIN(o.product_name) AS product_name
        FROM orders o
        LEFT JOIN asin_meta m ON o.asin = m.asin
        WHERE o.asin IS NOT NULL
          AND TRIM(o.asin) <> ''
          AND m.asin IS NULL
          AND o.product_name IS NOT NULL
          AND TRIM(o.product_name) <> ''
          AND TRIM(o.product_name) <> '-'
        GROUP BY o.asin
        """
    )
    rows = cur.fetchall()

    to_insert = []
    for asin, product_name in rows:
        name = _extract_name(product_name)
        name = _strip_brand_prefix(name, brand_name)
        if not name:
            continue
        to_insert.append((asin, name))

    if not to_insert:
        return

    with conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO asin_meta (asin, title_override)
            VALUES (?, ?)
            """,
            to_insert,
        )


def ingest_and_archive(conn, csv_path_str: str, brand_id: str):
    csv_path = Path(csv_path_str).resolve()

    ARCHIVE_DIR = BRAND_ARCHIVE_PATH(brand_id)

    row_count = apply_report_to_db(conn, csv_path)
    ensure_asin_meta(conn, brand_id)

    # Archive file with timestamp prefix
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived_name = f"{ts}__{csv_path.name}"
    archived_path = ARCHIVE_DIR / archived_name

    shutil.move(str(csv_path), archived_path)
    sha = file_sha256(archived_path)

    imported_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with conn:
        conn.execute("""
            INSERT INTO imports (
                original_path, archived_path, imported_at, row_count, file_sha256
            ) VALUES (?, ?, ?, ?, ?);
        """, (str(csv_path), str(archived_path), imported_at, row_count, sha))

    conn.close()
    print(f"Ingested {row_count} rows from {csv_path.name}, archived to {archived_path}")
