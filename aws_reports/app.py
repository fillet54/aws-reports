
from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict, List, Optional

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    abort,
    flash,
)

from .userdirs import user_data_dir  # adjust import if needed

app = Flask(__name__)
app.secret_key = "change-me"  # needed for flash()

# -------------------------------------------------------------------
# Persistence helpers
# -------------------------------------------------------------------

class Brand(TypedDict):
    id: str
    name: str


DATA_DIR: Path = user_data_dir("AwsReporting")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BRANDS_FILE: Path = DATA_DIR / "brands.json"


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
    return render_template("brands/index.html", brand=brand)


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


if __name__ == "__main__":
    app.run(debug=True, port=8080)
