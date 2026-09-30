import re
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from apps.catalog.models import Brand
from apps.sales.ingest import ingest_report
from apps.sales.models import RawReport

# Files archived by the old Flask app are named "20250101T120000Z__original.txt".
ARCHIVE_STAMP = re.compile(r"^(\d{8}T\d{6}Z)__")


def fetched_at_for(path: Path) -> datetime:
    match = ARCHIVE_STAMP.match(path.name)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


class Command(BaseCommand):
    help = "Import Amazon 'All Orders' flat-file reports for a brand, oldest first."

    def add_arguments(self, parser):
        parser.add_argument("brand", help="Brand slug.")
        parser.add_argument("files", nargs="+", type=Path)

    def handle(self, *args, brand, files, **options):
        try:
            brand_obj = Brand.objects.get(slug=brand)
        except Brand.DoesNotExist:
            raise CommandError(f"No brand with slug {brand!r}.")
        import_files(self, brand_obj, files, RawReport.Source.UPLOAD)


def import_files(command, brand, files, source):
    for path in sorted(files, key=fetched_at_for):
        result = ingest_report(
            brand,
            path.read_bytes(),
            source=source,
            fetched_at=fetched_at_for(path),
            original_filename=path.name,
        )
        status = "already imported" if result.duplicate else f"{result.new_versions} new/changed orders"
        command.stdout.write(f"{brand.slug}: {path.name}: {status}")
