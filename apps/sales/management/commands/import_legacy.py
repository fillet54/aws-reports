import json
import sqlite3
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils.text import slugify

from apps.catalog.models import Brand, Marketplace, Product
from apps.sales.models import RawReport

from .import_report import import_files


class Command(BaseCommand):
    help = (
        "Import data from the old Flask app's data directory: brands.json, "
        "per-brand ASIN metadata and every archived report file."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "data_dir", type=Path, help="The old DATA_DIR, e.g. ~/.local/share/aws-reporting"
        )

    def handle(self, *args, data_dir: Path, **options):
        brands_file = data_dir / "brands.json"
        if not brands_file.exists():
            raise CommandError(f"{brands_file} not found.")

        marketplaces = list(Marketplace.objects.all())
        for item in json.loads(brands_file.read_text()):
            old_id, name = str(item["id"]), item["name"]
            brand, created = Brand.objects.get_or_create(
                slug=slugify(old_id) or slugify(name), defaults={"name": name}
            )
            if created:
                brand.marketplaces.set(marketplaces)
            self.stdout.write(f"Brand {brand.slug} ({'created' if created else 'exists'})")

            brand_dir = data_dir / "brands" / old_id
            archive = brand_dir / "archive"
            if archive.is_dir():
                import_files(self, brand, [p for p in archive.iterdir() if p.is_file()], RawReport.Source.LEGACY)

            db_path = brand_dir / "orders.sqlite"
            if db_path.exists():
                self.import_asin_meta(brand, db_path)

    def import_asin_meta(self, brand, db_path: Path):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("SELECT * FROM asin_meta").fetchall()
        except sqlite3.OperationalError:
            return
        finally:
            conn.close()
        for row in rows:
            product, _ = Product.objects.get_or_create(brand=brand, asin=row["asin"][:10])
            product.title = row["title_override"] or product.title
            product.category = row["category"] or product.category
            if row["cost"] is not None:
                product.unit_cost = row["cost"]
            product.notes = row["notes"] or product.notes
            product.save()
        self.stdout.write(f"  {len(rows)} product names imported")
