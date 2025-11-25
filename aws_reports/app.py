
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TypedDict

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    abort,
    flash,
)

from . import ingest, reports, asin_meta
from .config import BRANDS_FILE, UPLOAD_TMP_DIR
from .db import get_brand_db

app = Flask(__name__)
app.secret_key = "change-me"  # needed for flash()

# -------------------------------------------------------------------
# Persistence helpers
# -------------------------------------------------------------------

class Brand(TypedDict):
    id: str
    name: str


def load_brands() -> List[Brand]:
    if not BRANDS_FILE.exists():
        return []
    try:
        with BRANDS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        # ensure it's a list of dicts with id+name
        brands: List[Brand] = []
        for item in data:
            if isinstance(item, dict) and "id" in item and "name" in item:
                brands.append({"id": str(item["id"]), "name": str(item["name"])})
        return brands
    except Exception:
        # if file is corrupted, you might want to log this
        return []


def save_brands(brands: List[Brand]) -> None:
    with BRANDS_FILE.open("w", encoding="utf-8") as f:
        json.dump(brands, f, indent=2)


def find_brand(brands: List[Brand], brand_id: str) -> Optional[Brand]:
    for b in brands:
        if b["id"] == brand_id:
            return b
    return None


def get_brand_or_404(brand_id: str) -> Brand:
    brands = load_brands()
    brand = find_brand(brands, brand_id)
    if brand is None:
        abort(404)
    return brand


def _safe_next_url() -> Optional[str]:
    """
    Return a safe in-app URL for redirects (only allow absolute paths).
    """
    candidate = request.form.get("next") or request.args.get("next")
    if candidate and candidate.startswith("/") and not candidate.startswith("//"):
        return candidate
    return None


# -------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------

@app.route("/")
def index():
    brands = load_brands()
    return render_template("index.html", brands=brands)


@app.route("/choose-brand", methods=["POST"])
def choose_brand():
    brand_id = request.form.get("brand_id", "").strip()
    if not brand_id:
        flash("Please select a brand.", "error")
        return redirect(url_for("index"))
    return redirect(url_for("brand_index", brand_id=brand_id))


@app.route("/brands/<brand_id>")
def brand_index(brand_id: str):
    brand = get_brand_or_404(brand_id)

    today = datetime.utcnow().date()
    ytd_start = today.replace(month=1, day=1)
    mtd_start = today.replace(day=1)
    week_start = today - timedelta(days=today.weekday())  # Monday of current week
    # Prior-year date helpers (approx by subtracting 365 days to keep ranges aligned)
    year_offset = timedelta(days=365)
    ytd_start_prev = ytd_start - year_offset
    mtd_start_prev = mtd_start - year_offset
    week_start_prev = week_start - year_offset
    today_prev = today - year_offset

    conn = get_brand_db(brand_id)
    try:
        ytd_sales_all = reports.get_sales_total(conn, ytd_start.isoformat(), today.isoformat())
        mtd_sales_all = reports.get_sales_total(conn, mtd_start.isoformat(), today.isoformat())
        week_sales_all = reports.get_sales_total(conn, week_start.isoformat(), today.isoformat())
        ytd_sales_prev_all = reports.get_sales_total(conn, ytd_start_prev.isoformat(), today_prev.isoformat())
        mtd_sales_prev_all = reports.get_sales_total(conn, mtd_start_prev.isoformat(), today_prev.isoformat())
        week_sales_prev_all = reports.get_sales_total(conn, week_start_prev.isoformat(), today_prev.isoformat())

        ytd_sales_by_channel = reports.get_sales_total_by_channel(conn, ytd_start.isoformat(), today.isoformat())
        mtd_sales_by_channel = reports.get_sales_total_by_channel(conn, mtd_start.isoformat(), today.isoformat())
        week_sales_by_channel = reports.get_sales_total_by_channel(conn, week_start.isoformat(), today.isoformat())
        ytd_sales_prev_by_channel = reports.get_sales_total_by_channel(conn, ytd_start_prev.isoformat(), today_prev.isoformat())
        mtd_sales_prev_by_channel = reports.get_sales_total_by_channel(conn, mtd_start_prev.isoformat(), today_prev.isoformat())
        week_sales_prev_by_channel = reports.get_sales_total_by_channel(conn, week_start_prev.isoformat(), today_prev.isoformat())

        latest_updated_date = reports.get_latest_last_updated_date(conn)
        channel_chart_data = reports.get_yearly_channel_monthly_totals(conn, today.year)
    finally:
        conn.close()

    sales_summary = {
        "ytd": ytd_sales_all,
        "mtd": mtd_sales_all,
        "week": week_sales_all,
        "today": today.isoformat(),
        "week_start": week_start.isoformat(),
        "mtd_start": mtd_start.isoformat(),
        "ytd_start": ytd_start.isoformat(),
        "ytd_prev": ytd_sales_prev_all,
        "mtd_prev": mtd_sales_prev_all,
        "week_prev": week_sales_prev_all,
    }

    def calc_change(curr: float, prev: float):
        if prev == 0:
            return None
        return ((curr - prev) / prev) * 100.0

    channel_sales_summary = {}
    for channel_key, currency in (("US", "USD"), ("CANADA", "CAD")):
        ytd_prev = ytd_sales_prev_by_channel.get(channel_key, 0.0)
        mtd_prev = mtd_sales_prev_by_channel.get(channel_key, 0.0)
        week_prev = week_sales_prev_by_channel.get(channel_key, 0.0)
        channel_sales_summary[channel_key] = {
            "currency": currency,
            "ytd": ytd_sales_by_channel.get(channel_key, 0.0),
            "ytd_prev": ytd_prev,
            "ytd_change": calc_change(ytd_sales_by_channel.get(channel_key, 0.0), ytd_prev),
            "mtd": mtd_sales_by_channel.get(channel_key, 0.0),
            "mtd_prev": mtd_prev,
            "mtd_change": calc_change(mtd_sales_by_channel.get(channel_key, 0.0), mtd_prev),
            "week": week_sales_by_channel.get(channel_key, 0.0),
            "week_prev": week_prev,
            "week_change": calc_change(week_sales_by_channel.get(channel_key, 0.0), week_prev),
            "today": today.isoformat(),
            "today_prev": today_prev.isoformat(),
            "week_start": week_start.isoformat(),
            "week_start_prev": week_start_prev.isoformat(),
            "mtd_start": mtd_start.isoformat(),
            "mtd_start_prev": mtd_start_prev.isoformat(),
            "ytd_start": ytd_start.isoformat(),
            "ytd_start_prev": ytd_start_prev.isoformat(),
        }

    return render_template(
        "brands/index.html",
        brand=brand,
        sales_summary=sales_summary,
        latest_updated_date=latest_updated_date,
        channel_chart_data=channel_chart_data,
        channel_sales_summary=channel_sales_summary,
    )


# -------------------------------------------------------------------
# Create / Update brand
# -------------------------------------------------------------------

@app.route("/brands/manage")
def manage_brands():
    """Simple page listing brands with a 'new brand' button."""
    brands = load_brands()
    return render_template("brands/manage.html", brands=brands)


@app.route("/brands/new")
def new_brand():
    """Show empty form to create a brand."""
    brand: Brand = {"id": "", "name": ""}
    return render_template("brands/edit.html", brand=brand, is_new=True)


@app.route("/brands/<brand_id>/edit")
def edit_brand(brand_id: str):
    """Show form to edit existing brand."""
    brand = get_brand_or_404(brand_id)
    return render_template("brands/edit.html", brand=brand, is_new=False)


@app.route("/brands/save", methods=["POST"])
def save_brand():
    """
    Create or update a brand.

    - If `original_id` is present and matches an existing brand, update that brand.
    - Otherwise, if `brand_id` matches an existing brand, update it.
    - Otherwise, create a new brand.
    """
    original_id = request.form.get("original_id", "").strip()
    brand_id = request.form.get("brand_id", "").strip()
    name = request.form.get("name", "").strip()

    if not brand_id or not name:
        flash("Brand ID and Name are required.", "error")
        # If we had an original_id, go back to edit; else new
        if original_id:
            return redirect(url_for("edit_brand", brand_id=original_id))
        return redirect(url_for("new_brand"))

    brands = load_brands()

    # Prefer original_id for locating existing brand (in case user changed ID)
    target_id = original_id or brand_id
    existing = find_brand(brands, target_id)

    if existing:
        # Update existing
        existing["id"] = brand_id
        existing["name"] = name
        flash("Brand updated.", "success")
    else:
        # Ensure no duplicate ids
        if find_brand(brands, brand_id):
            flash("Brand ID already exists.", "error")
            return redirect(url_for("new_brand"))

        brands.append({"id": brand_id, "name": name})
        flash("Brand created.", "success")

    save_brands(brands)

    return redirect(url_for("brand_index", brand_id=brand_id))


# -------------------------------------------------------------------
# Import / View Brand Orders
# -------------------------------------------------------------------

@app.route("/brands/<brand_id>/import", methods=["POST"])
def import_orders_report(brand_id: str):
    """
    Upload a CSV orders report for this brand and import it.

    Steps:
      1. Save the uploaded file to a temp location in the user data dir.
      2. Open the SQLite DB.
      3. Ensure schema via db.init_db(conn).
      4. Call ingest.ingest_and_archive(conn, csv_path).
    """
    brand = get_brand_or_404(brand_id)  # 404 if brand doesn't exist

    file = request.files.get("report_file")
    if not file or file.filename == "":
        flash("Please choose a CSV file to upload.", "error")
        return redirect(url_for("brand_index", brand_id=brand["id"]))

    # Very simple extension check, you can make this stricter if you like
    filename = file.filename
    lowered = filename.lower()
    if not (lowered.endswith(".csv") or lowered.endswith(".txt")):
        flash("Only .csv or .txt files are supported.", "error")
        return redirect(url_for("brand_index", brand_id=brand["id"]))

    # Create a unique temp file name (brand + timestamp + original name)
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    safe_name = filename.replace("/", "_").replace("\\", "_")
    tmp_path = UPLOAD_TMP_DIR / f"{brand['id']}-{ts}-{safe_name}"

    file.save(tmp_path)

    try:
        # Open SQLite DB
        conn = get_brand_db(brand_id)

        try:
            # Ingest + archive report
            # (Assuming ingest_and_archive does its own archiving of tmp_path)
            ingest.ingest_and_archive(conn, tmp_path, brand_id)

            conn.commit()
            flash("Orders report imported successfully.", "success")
        finally:
            conn.close()

    except Exception as exc:
        # In a real app you'd log this
        flash(f"Failed to import report: {exc}", "error")

    return redirect(url_for("brand_index", brand_id=brand["id"]))


@app.route("/brands/<brand_id>/reports")
def brand_reports(brand_id: str):
    """
    Show monthly status summary for this brand.
    Uses reports.get_monthly_status_summary(conn, n_months).
    """
    brand = get_brand_or_404(brand_id)
    channel = request.args.get("channel", "US").upper()
    if channel not in ("US", "CANADA"):
        channel = "US"
    selected_month = request.args.get("month")

    # How many months to show (you can make this configurable / query param)
    n_months = 12

    conn = get_brand_db(brand_id)
    try:

        # Get list of month summaries ordered latest -> earliest
        monthly_summaries: List[Dict[str, Any]] = reports.get_monthly_status_summary(
            conn, n_months=n_months, channel=channel
        )
    finally:
        conn.close()

    month_options = [m["year_month"] for m in monthly_summaries]
    if selected_month and selected_month not in month_options:
        selected_month = None
    if not selected_month and month_options:
        selected_month = month_options[0]
    if selected_month:
        monthly_summaries = [m for m in monthly_summaries if m["year_month"] == selected_month]

    is_partial = request.headers.get("HX-Request") or request.args.get("partial") == "1"
    template = "brands/partials/monthly_report.html" if is_partial else "brands/reports.html"
    return render_template(
        template,
        brand=brand,
        monthly_summaries=monthly_summaries,
        n_months=n_months,
        current_channel=channel,
        month_options=month_options,
        selected_month=selected_month,
    )


@app.route("/brands/<brand_id>/reports/weekly")
def brand_weekly_reports(brand_id: str):
    """
    Show weekly status summary for a configurable date range.
    """
    brand = get_brand_or_404(brand_id)
    channel = request.args.get("channel", "US").upper()
    if channel not in ("US", "CANADA"):
        channel = "US"
    selected_week = request.args.get("week")

    today = datetime.utcnow().date()
    default_start = (today - timedelta(days=28)).isoformat()
    default_end = today.isoformat()

    input_start = request.args.get("start_date", default_start)
    input_end = request.args.get("end_date", default_end)

    try:
        start_date = datetime.fromisoformat(input_start).date()
        end_date = datetime.fromisoformat(input_end).date()
        if start_date > end_date:
            raise ValueError("Start date must be before end date.")
    except ValueError:
        flash(
            "Invalid date range. Use YYYY-MM-DD and ensure the start date precedes the end date.",
            "error",
        )
        start_date = datetime.fromisoformat(default_start).date()
        end_date = datetime.fromisoformat(default_end).date()

    conn = get_brand_db(brand_id)
    try:
        weekly_summaries: List[Dict[str, Any]] = reports.get_weekly_status_summary(
            conn,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            channel=channel,
        )
    except ValueError as exc:
        flash(str(exc), "error")
        weekly_summaries = []
    finally:
        conn.close()

    week_options = [
        {
            "label": w["week_label"],
            "start": w["week_start"],
            "end": w["week_end"],
            "display": f"{w['week_label']} — {w['week_start']} → {w['week_end']}",
        }
        for w in weekly_summaries
    ]
    labels = [opt["label"] for opt in week_options]
    if selected_week and selected_week not in labels:
        selected_week = None
    if not selected_week and week_options:
        # weekly_summaries are sorted ascending; pick latest
        selected_week = week_options[-1]["label"]
    if selected_week:
        weekly_summaries = [w for w in weekly_summaries if w["week_label"] == selected_week]

    is_partial = request.headers.get("HX-Request") or request.args.get("partial") == "1"
    template = "brands/partials/weekly_report.html" if is_partial else "brands/weekly_reports.html"
    return render_template(
        template,
        brand=brand,
        weekly_summaries=weekly_summaries,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        current_channel=channel,
        week_options=week_options,
        selected_week=selected_week,
    )

# -------------------------------------------------------------------
# CRUD for ASINs 
# -------------------------------------------------------------------

@app.route("/brands/<brand_id>/asin-meta")
def asin_meta_index(brand_id: str):
    brand = get_brand_or_404(brand_id)
    conn = get_brand_db(brand_id)
    try:
        items = asin_meta.get_all_asin_meta(conn)
    finally:
        conn.close()

    return render_template("asin_meta/index.html", brand=brand, items=items)


@app.route("/brands/<brand_id>/asin-meta/new")
def asin_meta_new(brand_id: str):
    brand = get_brand_or_404(brand_id)
    item = {
        "asin": "",
        "title_override": "",
        "brand": "",
        "category": "",
        "subcategory": "",
        "cost": "",
        "launch_date": "",
        "notes": "",
    }
    return render_template("asin_meta/edit.html", brand=brand, item=item, is_new=True, next_url=_safe_next_url())



@app.route("/brands/<brand_id>/asin-meta/<asin>/edit")
def asin_meta_edit(brand_id: str, asin: str):
    brand = get_brand_or_404(brand_id)
    conn = get_brand_db(brand_id)
    try:
        item = asin_meta.get_asin_meta(conn, asin)
    finally:
        conn.close()

    if not item:
        abort(404)

    # Convert None to "" for form fields
    for key in ["title_override", "brand", "category", "subcategory", "launch_date", "notes"]:
        item[key] = item[key] or ""
    # cost might be None
    item["cost"] = "" if item["cost"] is None else item["cost"]

    return render_template("asin_meta/edit.html", brand=brand, item=item, is_new=False, next_url=_safe_next_url())


@app.route("/brands/<brand_id>/asin-meta/save", methods=["POST"])
def asin_meta_save(brand_id: str):
    next_url = _safe_next_url()
    brand = get_brand_or_404(brand_id)
    original_asin = request.form.get("original_asin", "").strip()
    asin_value = request.form.get("asin", "").strip()
    title_override = request.form.get("title_override", "").strip()
    brand_value = request.form.get("brand", "").strip()
    category = request.form.get("category", "").strip()
    subcategory = request.form.get("subcategory", "").strip()
    cost_raw = request.form.get("cost", "").strip()
    launch_date = request.form.get("launch_date", "").strip()
    notes = request.form.get("notes", "").strip()

    if not asin_value:
        flash("ASIN is required.", "error")
        if original_asin:
            return redirect(url_for("asin_meta_edit", brand_id=brand_id, asin=original_asin, next=next_url))
        return redirect(url_for("asin_meta_new" , brand_id=brand_id, next=next_url))

    # cost is optional
    cost: Any
    if cost_raw == "":
        cost = None
    else:
        try:
            cost = float(cost_raw)
        except ValueError:
            flash("Cost must be a number.", "error")
            if original_asin:
                return redirect(url_for("asin_meta_edit", brand_id=brand_id, asin=original_asin, next=next_url))
            return redirect(url_for("asin_meta_new", brand_id=brand_id, next=next_url))

    conn = get_brand_db(brand_id) 
    try:

        # If ASIN changed, delete the old row first
        if original_asin and original_asin != asin_value:
            asin_meta.delete_asin_meta(conn, original_asin)

        data: Dict[str, Any] = {
            "asin": asin_value,
            "title_override": title_override or None,
            "brand": brand_value or None,
            "category": category or None,
            "subcategory": subcategory or None,
            "cost": cost,
            "launch_date": launch_date or None,  # store as TEXT, e.g. "2025-01-01"
            "notes": notes or None,
        }

        asin_meta.upsert_asin_meta(conn, data)
        conn.commit()
    finally:
        conn.close()

    flash("ASIN metadata saved.", "success")
    if next_url:
        return redirect(next_url)
    return redirect(url_for("asin_meta_index", brand_id=brand_id))


@app.route("/brands/<brand_id>/asin-meta/<asin>/delete", methods=["POST"])
def asin_meta_delete_route(brand_id: str, asin: str):
    brand = get_brand_or_404(brand_id)
    conn = get_brand_db(brand_id)
    try:
        asin_meta.delete_asin_meta(conn, asin)
        conn.commit()
    finally:
        conn.close()

    flash(f"Deleted ASIN {asin}.", "success")
    return redirect(url_for("asin_meta_index", brand_id=brand_id))


if __name__ == "__main__":
    app.run(debug=True, port=8080, host='0.0.0.0')
