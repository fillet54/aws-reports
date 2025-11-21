from typing import List, Dict, Any

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
            COALESCE(o.net_item_revenue, 0.0) AS net_item_revenue,
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

    months: Dict[str, Dict[str, Any]] = {}

    for (
        year_month,
        asin,
        item_status,
        qty,
        total,
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
                "statuses": empty_status_buckets(),
            }

        asin_entry = by_asin[asin]["statuses"]

        asin_entry[status_bucket]["units"] += int(qty)
        asin_entry[status_bucket]["total_sales"] += float(total)

        month_entry["totals"][status_bucket]["units"] += int(qty)
        month_entry["totals"][status_bucket]["total_sales"] += float(total)

    # order latest -> earliest by YYYY-MM string
    result = sorted(months.values(), key=lambda m: m["year_month"], reverse=True)
    return result
