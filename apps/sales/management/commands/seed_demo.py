from datetime import date

from django.core.management.base import BaseCommand

from apps.accounts.models import User
from apps.catalog.models import Brand, Marketplace
from apps.sales.amazon import DummyAmazonClient
from apps.sales.sync import backfill_brand, sync_brand

DEMO_BRANDS = [
    ("Northwind Naturals", "northwind"),
    ("Lumen Skin", "lumen"),
    ("Harbor & Pine", "harbor-pine"),
]


class Command(BaseCommand):
    help = "Create demo brands, users and fake order history (development only)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--days", type=int, help="Days of history (default: back to January 1 of last year)."
        )

    def handle(self, *args, days, **options):
        if days is None:
            today = date.today()
            days = (today - date(today.year - 1, 1, 1)).days + 7
        marketplaces = list(Marketplace.objects.all())
        client = DummyAmazonClient()

        for name, slug in DEMO_BRANDS:
            brand, created = Brand.objects.get_or_create(slug=slug, defaults={"name": name})
            brand.marketplaces.set(marketplaces)
            if created or not brand.raw_reports.exists():
                self.stdout.write(f"Loading {days} days of history for {name}…")
                backfill_brand(brand, days, client, simulate_history=True)
                sync_brand(brand, client)
            else:
                self.stdout.write(f"{name} already has data; skipping.")

        self._user("admin", "admin", role=User.Role.EMPLOYEE, superuser=True)
        self._user("employee", "employee", role=User.Role.EMPLOYEE)
        self._user("client", "client", role=User.Role.CLIENT, brand=Brand.objects.get(slug="northwind"))

        self.stdout.write(self.style.SUCCESS(
            "Demo ready. Log in as admin/admin, employee/employee or client/client (sees Northwind only)."
        ))

    def _user(self, username, password, role, brand=None, superuser=False):
        if User.objects.filter(username=username).exists():
            return
        user = User(username=username, role=role, brand=brand, is_superuser=superuser, is_staff=superuser)
        user.set_password(password)
        user.save()
