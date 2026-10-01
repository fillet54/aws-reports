import time

from django.core.management.base import BaseCommand, CommandError

from apps.catalog.models import Brand
from apps.performance.clients import get_performance_client
from apps.performance.sync import STORE, sync_performance


class Command(BaseCommand):
    help = "Fetch traffic, ads, DSP and Subscribe & Save data for every sync-enabled brand."

    def add_arguments(self, parser):
        parser.add_argument("--brand", help="Only this brand slug.")
        parser.add_argument("--client", choices=["dummy", "sp_api"], help="Override AMAZON_CLIENT.")
        parser.add_argument("--only", choices=list(STORE), action="append", help="Only these sources (repeatable).")
        parser.add_argument(
            "--days", type=int,
            help="Backfill this many days (ads are limited by Amazon to ~60-95 days back).",
        )
        parser.add_argument("--every", type=int, metavar="SECONDS", help="Keep running, every N seconds.")

    def handle(self, *args, **options):
        while True:
            failed = self.run_once(options)
            if not options["every"]:
                if failed:
                    raise CommandError(f"Some syncs failed: {', '.join(failed)}")
                return
            time.sleep(options["every"])

    def run_once(self, options):
        brands = Brand.objects.filter(sync_enabled=True)
        if options["brand"]:
            brands = Brand.objects.filter(slug=options["brand"])
            if not brands:
                raise CommandError(f"No brand with slug {options['brand']!r}.")
        client = get_performance_client(options["client"])
        kinds = tuple(options["only"] or STORE)
        failed = []
        for brand in brands:
            result = sync_performance(brand, client, days=options["days"], kinds=kinds)
            parts = [f"{k} {d} day(s)" for k, d in result.days.items()]
            parts += [f"{k} skipped ({why})" for k, why in result.skipped.items()]
            parts += [f"{k} FAILED ({err})" for k, err in result.failed.items()]
            self.stdout.write(f"{brand.slug}: " + "; ".join(parts))
            failed += [f"{brand.slug}/{k}" for k in result.failed]
        return failed
