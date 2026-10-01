"""
Load a folder of saved Seller Central "All Orders" exports for one brand.

Files are imported oldest first, as if each had just been downloaded from
Amazon at that time, so order history (Pending -> Shipped, cancellations)
builds up the same way the hourly API sync will build it.
"""

import re
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.utils.text import slugify

from apps.catalog.models import Brand, Marketplace
from apps.sales.ingest import decode_report, ingest_report, parse_datetime, parse_rows
from apps.sales.models import RawReport

# A timestamp at the start of a file name, e.g. "20250101T120000Z__orders.txt".
NAME_STAMP = re.compile(r"^(\d{8}T\d{6}Z)")
REPORT_SUFFIXES = {".txt", ".tsv", ".csv", ".gz"}


def fetched_at_for(path: Path) -> tuple[datetime, str]:
    """
    When the data in a file was current, and how we know:

    1. a timestamp at the start of the file name,
    2. otherwise the newest last-updated-date inside the file (the export
       can't be older than that),
    3. otherwise the file's modification time.
    """
    match = NAME_STAMP.match(path.name)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc), "file name"
    try:
        rows = parse_rows(decode_report(path.read_bytes()))
    except ValueError:
        rows = []
    updated = [d for d in (parse_datetime(r.get("last-updated-date", "")) for r in rows) if d]
    if updated:
        return max(updated), "last-updated-date"
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc), "file time"


def collect_files(paths: list[Path]) -> list[Path]:
    files = []
    for path in paths:
        if path.is_dir():
            files.extend(
                p for p in path.rglob("*")
                if p.is_file() and not p.name.startswith(".") and p.suffix.lower() in REPORT_SUFFIXES
            )
        elif path.is_file():
            files.append(path)
        else:
            raise CommandError(f"{path} doesn't exist.")
    return files


class Command(BaseCommand):
    help = (
        "Import saved Seller Central 'All Orders' exports for a brand, oldest first. "
        "Accepts files or folders. Files already imported are skipped, so you can "
        "step through a folder with --limit 1 and look at the app between runs."
    )

    def add_arguments(self, parser):
        parser.add_argument("brand", help="Brand slug, e.g. acme.")
        parser.add_argument("paths", nargs="+", type=Path, help="Export files or folders of them.")
        parser.add_argument("--limit", type=int, help="Import at most this many new files, then stop.")
        parser.add_argument(
            "--create",
            metavar="NAME",
            help="Create the brand with this display name (US and CA stores) if it doesn't exist.",
        )

    def handle(self, *args, brand, paths, limit, create, **options):
        brand_obj = Brand.objects.filter(slug=brand).first()
        if brand_obj is None:
            if not create:
                raise CommandError(f"No brand with slug {brand!r}. Add --create \"Brand Name\" to create it.")
            if slugify(brand) != brand:
                raise CommandError(f"{brand!r} isn't a valid slug; try {slugify(brand)!r}.")
            brand_obj = Brand.objects.create(slug=brand, name=create)
            brand_obj.marketplaces.set(Marketplace.objects.all())
            self.stdout.write(f"Created brand {create} ({brand}).")

        files = collect_files(paths)
        if not files:
            raise CommandError("No .txt/.tsv/.csv/.gz files found.")
        import_files(self, brand_obj, files, limit=limit)


def import_files(command, brand, files, limit=None):
    dated = sorted(((fetched_at_for(p), p) for p in files), key=lambda item: item[0][0])
    imported = skipped = failed = 0
    for (fetched_at, how), path in dated:
        if limit is not None and imported >= limit:
            break
        try:
            result = ingest_report(
                brand,
                path.read_bytes(),
                source=RawReport.Source.EXPORT,
                fetched_at=fetched_at,
                original_filename=path.name,
            )
        except ValueError as exc:
            failed += 1
            command.stderr.write(command.style.ERROR(f"{path.name}: {exc}"))
            continue

        if result.duplicate:
            skipped += 1
            continue
        imported += 1
        report = result.report
        changed = report.new_versions - result.new_orders
        command.stdout.write(
            f"{path.name}  [{fetched_at:%Y-%m-%d %H:%M} UTC, from {how}]\n"
            f"    {report.row_count} rows, {report.order_count} orders: "
            f"{result.new_orders} new, {changed} changed, {report.order_count - report.new_versions} unchanged"
        )

    remaining = len(dated) - imported - skipped - failed
    summary = f"{brand.slug}: imported {imported}, already imported {skipped}"
    if failed:
        summary += f", failed {failed}"
    if limit is not None and remaining > 0:
        summary += f", {remaining} not checked yet (run again to continue)"
    command.stdout.write(command.style.SUCCESS(summary))
