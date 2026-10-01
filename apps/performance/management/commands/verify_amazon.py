from collections import Counter
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from apps.catalog.models import Brand
from apps.performance.clients import get_performance_client
from apps.performance.verify import FAIL, GROUPS, INFO, PASS, SKIP, WARN, Verifier
from apps.sales.amazon import get_client

ICONS = {PASS: "✓", WARN: "!", FAIL: "✗", SKIP: "–", INFO: "i"}


class Command(BaseCommand):
    help = (
        "Check the app's assumptions about Amazon's APIs against a real, authorized brand. "
        "Read-only apart from caching advertising profile IDs. Exits with an error if any check fails."
    )

    def add_arguments(self, parser):
        parser.add_argument("brand", help="Brand slug.")
        parser.add_argument("--only", choices=GROUPS, action="append", help="Only these groups (repeatable).")
        parser.add_argument("--save-dir", type=Path, help="Write every raw API response here.")
        parser.add_argument("--slow", action="store_true", help="Also probe the ads history limits (extra reports).")
        parser.add_argument(
            "--client", choices=["sp_api", "dummy"], default="sp_api",
            help="Which client to check (default: the real APIs; 'dummy' to try the command out).",
        )

    def handle(self, *args, brand, only, save_dir, slow, client, **options):
        brand_obj = Brand.objects.filter(slug=brand).first()
        if brand_obj is None:
            raise CommandError(f"No brand with slug {brand!r}.")

        styles = {PASS: self.style.SUCCESS, WARN: self.style.WARNING, FAIL: self.style.ERROR,
                  SKIP: lambda s: s, INFO: self.style.HTTP_INFO}
        current = {"group": None}

        def show(result):
            if result.group != current["group"]:
                current["group"] = result.group
                self.stdout.write(self.style.MIGRATE_HEADING(f"\n{result.group}"))
            line = f"  {ICONS[result.status]} {result.name}"
            self.stdout.write(styles[result.status](line))
            if result.detail:
                self.stdout.write(f"      {result.detail}")

        self.stdout.write(f"Checking {brand_obj.name} against the {client} client. Reports can take a few minutes each.")
        verifier = Verifier(
            brand_obj, get_client(client), get_performance_client(client),
            save_dir=save_dir, slow=slow, progress=show,
        )
        results = verifier.run(tuple(only or GROUPS))

        counts = Counter(r.status for r in results)
        summary = ", ".join(f"{counts[s]} {s}" for s in (PASS, WARN, FAIL, SKIP, INFO) if counts[s])
        self.stdout.write(f"\n{summary}")
        if save_dir:
            self.stdout.write(f"Raw responses saved in {save_dir}")
        if counts[FAIL]:
            raise CommandError(f"{counts[FAIL]} check(s) failed.")
