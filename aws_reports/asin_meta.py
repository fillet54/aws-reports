
from __future__ import annotations

from typing import Any, Dict, List, Optional
import sqlite3


ROW_SQL = """
SELECT asin,
       title_override,
       brand,
       category,
       subcategory,
       cost,
       launch_date,
       notes
FROM asin_meta
"""


def _rows_to_dicts(cur: sqlite3.Cursor) -> List[Dict[str, Any]]:
    columns = [col[0] for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def get_all_asin_meta(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    cur = conn.execute(ROW_SQL + " ORDER BY asin")
    return _rows_to_dicts(cur)


def get_asin_meta(conn: sqlite3.Connection, asin: str) -> Optional[Dict[str, Any]]:
    cur = conn.execute(ROW_SQL + " WHERE asin = ?", (asin,))
    row = cur.fetchone()
    if not row:
        return None
    columns = [col[0] for col in cur.description]
    return dict(zip(columns, row))


def upsert_asin_meta(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    """
    Insert or update asin_meta row.

    `data` should have keys:
      asin, title_override, brand, category, subcategory, cost, launch_date, notes
    """
    conn.execute(
        """
        INSERT INTO asin_meta (
            asin,
            title_override,
            brand,
            category,
            subcategory,
            cost,
            launch_date,
            notes
        )
        VALUES (
            :asin,
            :title_override,
            :brand,
            :category,
            :subcategory,
            :cost,
            :launch_date,
            :notes
        )
        ON CONFLICT(asin) DO UPDATE SET
            title_override = excluded.title_override,
            brand          = excluded.brand,
            category       = excluded.category,
            subcategory    = excluded.subcategory,
            cost           = excluded.cost,
            launch_date    = excluded.launch_date,
            notes          = excluded.notes
        """,
        data,
    )


def delete_asin_meta(conn: sqlite3.Connection, asin: str) -> None:
    conn.execute("DELETE FROM asin_meta WHERE asin = ?", (asin,))
