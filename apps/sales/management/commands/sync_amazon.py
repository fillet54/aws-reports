import time

from django.core.management.base import BaseCommand, CommandError

from apps.catalog.models import Brand
from apps.sales.amazon import get_client
from apps.sales.sync import backfill_brand, sync_brand


class Command(BaseCommand):
    help = "Fetch recent order changes from Amazon for every sync-enabled brand."

    def add_arguments(self, parser):
        parser.add_argument("--brand", help="Only sync this brand slug.")
        parser.add_argument("--client", choices=["dummy", "sp_api"], help="Override AMAZON_CLIENT.")
        parser.add_argument(
            "--backfill-days", type=int, help="Load this many days of history by order date instead."
        )
        parser.add_argument(
            "--every", type=int, metavar="SECONDS", help="Keep running, syncing every N seconds (e.g. 3600)."
        )

    def handle(self, *args, **options):
        while True:
            failed = self.run_once(options)
            if not options["every"]:
                if failed:
                    raise CommandError(f"Sync failed for: {', '.join(failed)}")
                return
            time.sleep(options["every"])

    def run_once(self, options):
        brands = Brand.objects.filter(sync_enabled=True)
        if options["brand"]:
            brands = Brand.objects.filter(slug=options["brand"])
            if not brands:
                raise CommandError(f"No brand with slug {options['brand']!r}.")

        client = get_client(options["client"])
        failed = []
        for brand in brands:
            try:
                if options["backfill_days"]:
                    result = backfill_brand(brand, options["backfill_days"], client)
                else:
                    result = sync_brand(brand, client)
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"{brand.slug}: {exc}"))
                failed.append(brand.slug)
                continue
            self.stdout.write(
                f"{brand.slug}: {result.reports} report(s), {result.new_versions} new/changed order(s)"
            )
        return failed
