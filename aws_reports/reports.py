from datetime import datetime
from typing import List, Dict, Any, Tuple

def get_monthly_status_summary(conn, n_months: int) -> List[Dict[str, Any]]:
    """
    Return past n_months of data as a list of month summaries, ordered
    from latest to earliest.

    Each month summary has:
      {
        "year_month": "YYYY-MM",
        "by_asin": {
           "<ASIN>": {
              "meta": {
                  "asin": "<ASIN>",
                  "title_override": ...,
                  "brand": ...,
                  "category": ...,
                  "subcategory": ...,
                  "cost": ...,
                  "launch_date": ...,
                  "notes": ...,
              },
              "statuses": {
                  "Shipped":   {"units": int, "total_sales": float},
                  "Unshipped": {"units": int, "total_sales": float},
                  "Cancelled": {"units": int, "total_sales": float},
              }
           },
           ...
        },
        "totals": {
           "Shipped":   {"units": int, "total_sales": float},
           "Unshipped": {"units": int, "total_sales": float},
           "Cancelled": {"units": int, "total_sales": float},
        }
      }
    """
    if n_months < 1:
        raise ValueError("n_months must be >= 1")

    cur = conn.cursor()

    # "Past N months" = from start of (N-1) months ago through now.
    offset = -(n_months - 1)  # 1 -> 0 months, 3 -> -2 months
    offset_expr = f"{offset} months"

    # Pull rows + meta via LEFT JOIN
    cur.execute(
        """
        SELECT
            strftime('%Y-%m', o.purchase_date) AS year_month,
            o.asin,
            o.item_status,
            COALESCE(o.quantity, 0)           AS quantity,
            COALESCE(o.quantity, 0) * (COALESCE(o.item_price, 0.0) - COALESCE(o.item_promotion_discount, 0.0)) AS order_revenue,
            o.sales_channel,
            m.title_override,
            m.brand,
            m.category,
            m.subcategory,
            m.cost,
            m.launch_date,
            m.notes
        FROM orders o
        LEFT JOIN asin_meta m ON o.asin = m.asin
        WHERE o.purchase_date IS NOT NULL
          AND o.purchase_date >= date('now', 'start of month', ?)
          AND o.item_price IS NOT NULL
          AND (o.order_status IS NULL OR lower(o.order_status) <> 'pending')
          AND (o.item_status IS NULL OR lower(o.item_status) <> 'pending')
        """,
        (offset_expr,),
    )
    rows = cur.fetchall()

    def bucket_status(status: str):
        s = (status or "").strip().lower()
        if s == "shipped":
            return "Shipped"
        if s == "unshipped":
            return "Unshipped"
        if s in ("canceled", "cancelled"):
            return "Cancelled"
        # ignore others like Pending for this report
        return None

    def empty_status_buckets():
        return {
            "Shipped":   {"units": 0, "total_sales": 0.0},
            "Unshipped": {"units": 0, "total_sales": 0.0},
            "Cancelled": {"units": 0, "total_sales": 0.0},
        }

    def bucket_channel(channel: str | None):
        c = (channel or "").strip().lower()
        if c == "amazon.com":
            return "US"
        if c == "amazon.ca":
            return "CANADA"
        return None

    def empty_channel_buckets():
        return {
            "US": {"units": 0, "total_sales": 0.0},
            "CANADA": {"units": 0, "total_sales": 0.0},
        }

    months: Dict[str, Dict[str, Any]] = {}

    for (
        year_month,
        asin,
        item_status,
        qty,
        total,
        sales_channel,
        title_override,
        brand,
        category,
        subcategory,
        cost,
        launch_date,
        notes,
    ) in rows:
        status_bucket = bucket_status(item_status)
        if status_bucket is None:
            continue

        if year_month not in months:
            months[year_month] = {
                "year_month": year_month,
                "by_asin": {},
                "channel_totals": empty_channel_buckets(),
                "totals": empty_status_buckets(),
            }

        month_entry = months[year_month]
        by_asin = month_entry["by_asin"]

        if asin not in by_asin:
            # initialize asin entry with meta + empty status buckets
            by_asin[asin] = {
                "meta": {
                    "asin": asin,
                    "title_override": title_override,
                    "brand": brand,
                    "category": category,
                    "subcategory": subcategory,
                    "cost": cost,
                    "launch_date": launch_date,
                    "notes": notes,
                },
                "channels": empty_channel_buckets(),
                "statuses": empty_status_buckets(),
            }

        asin_entry = by_asin[asin]["statuses"]

        asin_entry[status_bucket]["units"] += int(qty)
        asin_entry[status_bucket]["total_sales"] += float(total)

        month_entry["totals"][status_bucket]["units"] += int(qty)
        month_entry["totals"][status_bucket]["total_sales"] += float(total)

        channel_bucket = bucket_channel(sales_channel)
        if channel_bucket:
            asin_channels = by_asin[asin]["channels"]
            month_channels = month_entry["channel_totals"]
            asin_channels[channel_bucket]["units"] += int(qty)
            asin_channels[channel_bucket]["total_sales"] += float(total)
            month_channels[channel_bucket]["units"] += int(qty)
            month_channels[channel_bucket]["total_sales"] += float(total)

    # order latest -> earliest by YYYY-MM string
    result = sorted(months.values(), key=lambda m: m["year_month"], reverse=True)
    return result


def get_weekly_status_summary(conn, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Return weekly summaries between start_date and end_date (inclusive)."""
    def parse_date_range(start: str, end: str) -> Tuple[str, str]:
        try:
            start_obj = datetime.fromisoformat(start).date()
            end_obj = datetime.fromisoformat(end).date()
        except ValueError as exc:
            raise ValueError("Dates must be in YYYY-MM-DD format.") from exc

        if start_obj > end_obj:
            raise ValueError("Start date must be on or before end date.")

        return start_obj.isoformat(), end_obj.isoformat()

    start_iso, end_iso = parse_date_range(start_date, end_date)

    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            date(o.purchase_date, 'weekday 1', '-7 days') AS week_start,
            date(o.purchase_date, 'weekday 1', '-7 days', '+6 days') AS week_end,
            strftime('%Y-W%W', o.purchase_date)            AS week_label,
            o.asin,
            o.item_status,
            COALESCE(o.quantity, 0)           AS quantity,
            COALESCE(o.quantity, 0) * (COALESCE(o.item_price, 0.0) - COALESCE(o.item_promotion_discount, 0.0)) AS order_revenue,
            o.sales_channel,
            m.title_override,
            m.brand,
            m.category,
            m.subcategory,
            m.cost,
            m.launch_date,
            m.notes
        FROM orders o
        LEFT JOIN asin_meta m ON o.asin = m.asin
        WHERE o.purchase_date IS NOT NULL
          AND date(o.purchase_date) BETWEEN ? AND ?
          AND o.item_price IS NOT NULL
          AND (o.order_status IS NULL OR lower(o.order_status) <> 'pending')
          AND (o.item_status IS NULL OR lower(o.item_status) <> 'pending')
        """,
        (start_iso, end_iso),
    )
    rows = cur.fetchall()

    def bucket_status(status: str):
        s = (status or "").strip().lower()
        if s == "shipped":
            return "Shipped"
        if s == "unshipped":
            return "Unshipped"
        if s in ("canceled", "cancelled"):
            return "Cancelled"
        return None

    def empty_status_buckets():
        return {
            "Shipped":   {"units": 0, "total_sales": 0.0},
            "Unshipped": {"units": 0, "total_sales": 0.0},
            "Cancelled": {"units": 0, "total_sales": 0.0},
        }

    def bucket_channel(channel: str | None):
        c = (channel or "").strip().lower()
        if c == "amazon.com":
            return "US"
        if c == "amazon.ca":
            return "CANADA"
        return None

    def empty_channel_buckets():
        return {
            "US": {"units": 0, "total_sales": 0.0},
            "CANADA": {"units": 0, "total_sales": 0.0},
        }

    weeks: Dict[str, Dict[str, Any]] = {}

    for (
        week_start,
        week_end,
        week_label,
        asin,
        item_status,
        qty,
        total,
        sales_channel,
        title_override,
        brand,
        category,
        subcategory,
        cost,
        launch_date,
        notes,
    ) in rows:
        status_bucket = bucket_status(item_status)
        if status_bucket is None:
            continue

        if week_label not in weeks:
            weeks[week_label] = {
                "week_label": week_label,
                "week_start": week_start,
                "week_end": week_end,
                "by_asin": {},
                "channel_totals": empty_channel_buckets(),
                "totals": empty_status_buckets(),
            }

        week_entry = weeks[week_label]
        by_asin = week_entry["by_asin"]

        if asin not in by_asin:
            by_asin[asin] = {
                "meta": {
                    "asin": asin,
                    "title_override": title_override,
                    "brand": brand,
                    "category": category,
                    "subcategory": subcategory,
                    "cost": cost,
                    "launch_date": launch_date,
                    "notes": notes,
                },
                "channels": empty_channel_buckets(),
                "statuses": empty_status_buckets(),
            }

        asin_entry = by_asin[asin]["statuses"]

        asin_entry[status_bucket]["units"] += int(qty)
        asin_entry[status_bucket]["total_sales"] += float(total)

        week_entry["totals"][status_bucket]["units"] += int(qty)
        week_entry["totals"][status_bucket]["total_sales"] += float(total)

        channel_bucket = bucket_channel(sales_channel)
        if channel_bucket:
            asin_channels = by_asin[asin]["channels"]
            week_channels = week_entry["channel_totals"]
            asin_channels[channel_bucket]["units"] += int(qty)
            asin_channels[channel_bucket]["total_sales"] += float(total)
            week_channels[channel_bucket]["units"] += int(qty)
            week_channels[channel_bucket]["total_sales"] += float(total)

    result = sorted(weeks.values(), key=lambda w: w["week_start"])
    return result


def get_sales_total(conn, start_date: str, end_date: str) -> float:
    """
    Return total revenue for orders between start_date and end_date
    (inclusive), excluding cancelled orders. Revenue is
    quantity * (item_price - item_promotion_discount); tax and shipping are ignored.
    Dates must be YYYY-MM-DD strings.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(
            COALESCE(quantity, 0) * (COALESCE(item_price, 0.0) - COALESCE(item_promotion_discount, 0.0))
        ), 0.0) AS total
        FROM orders
        WHERE purchase_date IS NOT NULL
          AND date(purchase_date) BETWEEN ? AND ?
          AND LOWER(COALESCE(item_status, '')) NOT IN ('cancelled', 'canceled')
          AND item_price IS NOT NULL
          AND (order_status IS NULL OR lower(order_status) <> 'pending')
          AND (item_status IS NULL OR lower(item_status) <> 'pending')
        """,
        (start_date, end_date),
    )
    row = cur.fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


def get_sales_total_by_channel(conn, start_date: str, end_date: str):
    """
    Like get_sales_total, but returns a dict keyed by channel (US, CANADA)
    using sales_channel to bucket values.
    """
    def bucket_channel(channel: str | None):
        c = (channel or "").strip().lower()
        if c == "amazon.com":
            return "US"
        if c == "amazon.ca":
            return "CANADA"
        return None

    totals = {"US": 0.0, "CANADA": 0.0}
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sales_channel,
               COALESCE(SUM(
                 COALESCE(quantity, 0) * (COALESCE(item_price, 0.0) - COALESCE(item_promotion_discount, 0.0))
               ), 0.0) AS total
        FROM orders
        WHERE purchase_date IS NOT NULL
          AND date(purchase_date) BETWEEN ? AND ?
          AND item_price IS NOT NULL
          AND LOWER(COALESCE(item_status, '')) NOT IN ('cancelled', 'canceled')
          AND (order_status IS NULL OR lower(order_status) <> 'pending')
          AND (item_status IS NULL OR lower(item_status) <> 'pending')
        GROUP BY sales_channel
        """,
        (start_date, end_date),
    )
    for channel, total in cur.fetchall():
        bucket = bucket_channel(channel)
        if bucket:
            totals[bucket] = float(total or 0.0)
    return totals


def get_latest_last_updated_date(conn) -> str | None:
    """Return the most recent date portion of last_updated_date in orders."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(date(last_updated_date))
        FROM orders
        WHERE last_updated_date IS NOT NULL
        """
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def get_yearly_channel_monthly_totals(conn, year: int):
    """
    Return per-month totals (units + sales) for each sales channel we care about.
    Channels are bucketed as:
      - Amazon.com -> US
      - Amazon.ca  -> CANADA
    Any other channels are ignored. Includes current year data plus previous year
    sales for comparison.
    """
    start = f"{year}-01-01"
    end = f"{year}-12-31"
    prev_year = year - 1
    prev_start = f"{prev_year}-01-01"
    prev_end = f"{prev_year}-12-31"

    def bucket_channel(channel: str | None):
        c = (channel or "").strip().lower()
        if c == "amazon.com":
            return "US"
        if c == "amazon.ca":
            return "CANADA"
        return None

    months = [f"{m:02d}" for m in range(1, 13)]

    def empty_months():
        return {m: {"units": 0, "sales": 0.0} for m in months}

    def collect(year_start: str, year_end: str):
        collected = {
            "US": empty_months(),
            "CANADA": empty_months(),
        }
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                strftime('%m', purchase_date) AS month_num,
                sales_channel,
                SUM(COALESCE(quantity, 0)) AS units,
                SUM(COALESCE(quantity, 0) * (COALESCE(item_price, 0.0) - COALESCE(item_promotion_discount, 0.0))) AS sales
            FROM orders
            WHERE purchase_date IS NOT NULL
              AND date(purchase_date) BETWEEN ? AND ?
              AND item_price IS NOT NULL
              AND LOWER(COALESCE(item_status, '')) NOT IN ('cancelled', 'canceled')
              AND (order_status IS NULL OR lower(order_status) <> 'pending')
              AND (item_status IS NULL OR lower(item_status) <> 'pending')
            GROUP BY month_num, sales_channel
            """,
            (year_start, year_end),
        )

        for month_num, sales_channel, units, sales in cur.fetchall():
            bucket = bucket_channel(sales_channel)
            if bucket is None or month_num is None:
                continue
            if month_num not in collected[bucket]:
                continue
            collected[bucket][month_num]["units"] += int(units or 0)
            collected[bucket][month_num]["sales"] += float(sales or 0.0)
        return collected

    current = collect(start, end)
    previous = collect(prev_start, prev_end)

    labels = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]

    def channel_series(key: str):
        curr = current.get(key, {})
        prev = previous.get(key, {})
        units_series = [curr.get(f"{i:02d}", {"units": 0})["units"] for i in range(1, 13)]
        sales_series = [curr.get(f"{i:02d}", {"sales": 0.0})["sales"] for i in range(1, 13)]
        prev_sales_series = [prev.get(f"{i:02d}", {"sales": 0.0})["sales"] for i in range(1, 13)]
        return {
            "units": units_series,
            "sales": sales_series,
            "previous_sales": prev_sales_series,
        }

    return {
        "labels": labels,
        "year": year,
        "previous_year": prev_year,
        "channels": {
            "US": channel_series("US"),
            "CANADA": channel_series("CANADA"),
        },
    }
