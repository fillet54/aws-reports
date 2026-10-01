"""
Daily performance numbers beyond orders: traffic (sessions, conversion,
offer count), advertising (sponsored ads and DSP) and Subscribe & Save.

Every API response is kept verbatim as a DataPull. The Daily* tables hold
the latest known value for each day; Amazon restates recent days (traffic
is finalized after a day or two, ad sales keep growing for 7-14 days after
a click), so re-fetching a day replaces that day's numbers.
"""

from django.db import models


class DataPull(models.Model):
    class Kind(models.TextChoices):
        TRAFFIC = "traffic", "Sales & traffic report"
        ADS = "ads", "Sponsored ads reports"
        DSP = "dsp", "DSP report"
        SUBSCRIPTIONS = "subscriptions", "Subscribe & Save metrics"

    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="data_pulls")
    marketplace = models.ForeignKey("catalog.Marketplace", on_delete=models.PROTECT, related_name="+")
    kind = models.CharField(max_length=16, choices=Kind.choices)
    source = models.CharField(max_length=16)
    start_date = models.DateField()
    end_date = models.DateField()
    fetched_at = models.DateTimeField(db_index=True)
    file = models.FileField(upload_to="data_pulls/%Y/%m/")
    days = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["-fetched_at"]


class DailyTraffic(models.Model):
    """One day of the Sales and Traffic report (Seller Central Business Reports)."""

    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="+")
    marketplace = models.ForeignKey("catalog.Marketplace", on_delete=models.PROTECT, related_name="+")
    date = models.DateField()
    ordered_product_sales = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    units_ordered = models.IntegerField(default=0)
    sessions = models.IntegerField(default=0)
    page_views = models.IntegerField(default=0)
    average_offer_count = models.DecimalField(max_digits=10, decimal_places=2, null=True)
    buy_box_percentage = models.DecimalField(max_digits=6, decimal_places=2, null=True)
    pull = models.ForeignKey(DataPull, on_delete=models.SET_NULL, null=True, related_name="+")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["brand", "marketplace", "date"], name="unique_daily_traffic")
        ]


class DailyAds(models.Model):
    class Product(models.TextChoices):
        SPONSORED_PRODUCTS = "SP", "Sponsored Products"
        SPONSORED_BRANDS = "SB", "Sponsored Brands"
        SPONSORED_DISPLAY = "SD", "Sponsored Display"
        DSP = "DSP", "DSP"

    SEARCH = ("SP", "SB", "SD")

    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="+")
    marketplace = models.ForeignKey("catalog.Marketplace", on_delete=models.PROTECT, related_name="+")
    date = models.DateField()
    ad_product = models.CharField(max_length=3, choices=Product.choices)
    cost = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    sales = models.DecimalField(
        max_digits=14, decimal_places=2, default=0,
        help_text="Attributed sales: 7-day for Sponsored Products, 14-day for the others.",
    )
    impressions = models.BigIntegerField(default=0)
    clicks = models.IntegerField(default=0)
    orders = models.IntegerField(default=0)
    units = models.IntegerField(default=0)
    pull = models.ForeignKey(DataPull, on_delete=models.SET_NULL, null=True, related_name="+")

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["brand", "marketplace", "date", "ad_product"], name="unique_daily_ads"
            )
        ]


class DailySubscriptions(models.Model):
    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="+")
    marketplace = models.ForeignKey("catalog.Marketplace", on_delete=models.PROTECT, related_name="+")
    date = models.DateField()
    active_subscriptions = models.IntegerField(default=0)
    shipped_units = models.IntegerField(default=0)
    revenue = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    pull = models.ForeignKey(DataPull, on_delete=models.SET_NULL, null=True, related_name="+")

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["brand", "marketplace", "date"], name="unique_daily_subscriptions")
        ]
