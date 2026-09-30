from decimal import Decimal

from django.db import migrations

MARKETPLACES = [
    # code, name, marketplace id, sales channel, currency, usd rate, sort
    ("US", "United States", "ATVPDKIKX0DER", "Amazon.com", "USD", Decimal("1"), 1),
    ("CA", "Canada", "A2EUQ1WTGCTBG2", "Amazon.ca", "CAD", Decimal("0.73"), 2),
]


def create_marketplaces(apps, schema_editor):
    Marketplace = apps.get_model("catalog", "Marketplace")
    for code, name, mid, channel, currency, rate, sort in MARKETPLACES:
        Marketplace.objects.get_or_create(
            code=code,
            defaults={
                "name": name,
                "amazon_marketplace_id": mid,
                "sales_channel": channel,
                "currency": currency,
                "usd_rate": rate,
                "sort_order": sort,
            },
        )


class Migration(migrations.Migration):
    dependencies = [("catalog", "0001_initial")]

    operations = [migrations.RunPython(create_marketplaces, migrations.RunPython.noop)]
