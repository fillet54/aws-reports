import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import shutil
import hashlib

from .config import BRAND_PATH 


def get_brand_db(brand_id: str):
    conn = sqlite3.connect(BRAND_PATH(brand_id) / "orders.sqlite")
    conn.row_factory = sqlite3.Row

    # For now just always init
    init_db(conn)

    return conn


def init_db(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS orders (
        id                            INTEGER PRIMARY KEY AUTOINCREMENT,

        amazon_order_id              TEXT NOT NULL,
        merchant_order_id            TEXT,
        purchase_date                TEXT,
        last_updated_date            TEXT NOT NULL,
        order_status                 TEXT,
        fulfillment_channel          TEXT,
        sales_channel                TEXT,
        order_channel                TEXT,
        url                          TEXT,
        ship_service_level           TEXT,
        product_name                 TEXT,
        sku                          TEXT,
        asin                         TEXT,
        item_status                  TEXT,
        quantity                     INTEGER,
        currency                     TEXT,
        item_price                   REAL,
        item_tax                     REAL,
        shipping_price               REAL,
        shipping_tax                 REAL,
        gift_wrap_price              REAL,
        gift_wrap_tax                REAL,
        item_promotion_discount      REAL,
        ship_promotion_discount      REAL,
        ship_city                    TEXT,
        ship_state                   TEXT,
        ship_postal_code             TEXT,
        ship_country                 TEXT,
        promotion_ids                TEXT,
        is_business_order            INTEGER,
        purchase_order_number        TEXT,
        price_designation            TEXT,
        buyer_identification_number  TEXT,
        buyer_identification_type    TEXT,

        gross_item_revenue AS (COALESCE(item_price,0) * COALESCE(quantity,1)),
        net_item_revenue   AS (
            COALESCE(item_price,0) * COALESCE(quantity,1)
            + COALESCE(item_tax,0)
            + COALESCE(shipping_price,0)
            + COALESCE(shipping_tax,0)
            + COALESCE(gift_wrap_price,0)
            + COALESCE(gift_wrap_tax,0)
            - COALESCE(item_promotion_discount,0)
            - COALESCE(ship_promotion_discount,0)
        )
    );

    CREATE INDEX IF NOT EXISTS idx_orders_amazon_order_id ON orders(amazon_order_id);
    CREATE INDEX IF NOT EXISTS idx_orders_asin           ON orders(asin);
    CREATE INDEX IF NOT EXISTS idx_orders_purchase_date ON orders(purchase_date);

    CREATE TABLE IF NOT EXISTS asin_meta (
        asin           TEXT PRIMARY KEY,
        title_override TEXT,
        brand          TEXT,
        category       TEXT,
        subcategory    TEXT,
        cost           REAL,
        launch_date    TEXT,
        notes          TEXT
    );

    CREATE TABLE IF NOT EXISTS imports (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        original_path    TEXT NOT NULL,
        archived_path    TEXT NOT NULL,
        imported_at      TEXT NOT NULL,
        row_count        INTEGER NOT NULL,
        file_sha256      TEXT
    );
    """)
    conn.commit()


