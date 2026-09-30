"""
Append-only storage for Amazon order data.

    RawReport     every report file we downloaded or uploaded, kept verbatim
    OrderVersion  one snapshot of one order, created only when its content changes
    OrderLine     the line items of that snapshot (one row per order item)

"Current" data is the newest OrderVersion of each order (is_current=True).
Data "as known at" some moment is the newest version fetched at or before it,
which is what frozen shared reports use.
"""

from django.db import models


class RawReport(models.Model):
    class Source(models.TextChoices):
        SP_API = "sp_api", "Amazon SP-API"
        DUMMY = "dummy", "Dummy Amazon client"
        UPLOAD = "upload", "Manual upload"
        LEGACY = "legacy", "Legacy import"

    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="raw_reports")
    source = models.CharField(max_length=16, choices=Source.choices)
    report_type = models.CharField(max_length=128, blank=True)
    data_start = models.DateTimeField(null=True, blank=True)
    data_end = models.DateTimeField(null=True, blank=True)
    fetched_at = models.DateTimeField(
        db_index=True, help_text="When this data was known to be true (download time)."
    )
    file = models.FileField(upload_to="raw_reports/%Y/%m/")
    original_filename = models.CharField(max_length=255, blank=True)
    sha256 = models.CharField(max_length=64)
    row_count = models.PositiveIntegerField(default=0)
    order_count = models.PositiveIntegerField(default=0)
    new_versions = models.PositiveIntegerField(
        default=0, help_text="Orders that were new or changed in this report."
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-fetched_at"]
        constraints = [
            models.UniqueConstraint(fields=["brand", "sha256"], name="unique_raw_report_per_brand")
        ]

    def __str__(self) -> str:
        return f"{self.brand} {self.get_source_display()} @ {self.fetched_at:%Y-%m-%d %H:%M}"


class OrderVersion(models.Model):
    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="order_versions")
    amazon_order_id = models.CharField(max_length=32)
    report = models.ForeignKey(RawReport, on_delete=models.CASCADE, related_name="order_versions")
    fetched_at = models.DateTimeField()
    last_updated_date = models.DateTimeField(null=True, blank=True)
    order_status = models.CharField(max_length=32, blank=True)
    content_hash = models.CharField(max_length=64)
    is_current = models.BooleanField(default=True)

    class Meta:
        indexes = [
            models.Index(fields=["brand", "amazon_order_id", "fetched_at"]),
            models.Index(fields=["brand", "is_current"]),
        ]

    def __str__(self) -> str:
        return f"{self.amazon_order_id} @ {self.fetched_at:%Y-%m-%d %H:%M}"


class OrderLine(models.Model):
    version = models.ForeignKey(OrderVersion, on_delete=models.CASCADE, related_name="lines")
    # Denormalized from the version so report queries don't need joins.
    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="+")
    marketplace = models.ForeignKey(
        "catalog.Marketplace", on_delete=models.PROTECT, null=True, blank=True, related_name="+"
    )
    amazon_order_id = models.CharField(max_length=32)

    purchase_date = models.DateTimeField(null=True)
    purchase_day = models.DateField(
        null=True, help_text="Purchase date in REPORT_TIME_ZONE; used for day/week/month grouping."
    )
    order_status = models.CharField(max_length=32, blank=True)
    item_status = models.CharField(max_length=32, blank=True)
    fulfillment_channel = models.CharField(max_length=32, blank=True)
    sales_channel = models.CharField(max_length=64, blank=True)

    asin = models.CharField(max_length=10, blank=True)
    sku = models.CharField(max_length=64, blank=True)
    product_name = models.TextField(blank=True)

    quantity = models.IntegerField(null=True)
    currency = models.CharField(max_length=3, blank=True)
    item_price = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    item_tax = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    shipping_price = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    item_promotion_discount = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    ship_promotion_discount = models.DecimalField(max_digits=12, decimal_places=2, null=True)
    is_business_order = models.BooleanField(null=True)

    # The full original row, so new columns can be promoted later without re-fetching.
    raw = models.JSONField()

    class Meta:
        indexes = [
            models.Index(fields=["brand", "purchase_day"]),
            models.Index(fields=["brand", "asin"]),
        ]


class SyncRun(models.Model):
    class Status(models.TextChoices):
        RUNNING = "running", "Running"
        SUCCESS = "success", "Success"
        FAILED = "failed", "Failed"

    brand = models.ForeignKey("catalog.Brand", on_delete=models.CASCADE, related_name="sync_runs")
    client = models.CharField(max_length=16)
    window_start = models.DateTimeField()
    window_end = models.DateTimeField()
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.RUNNING)
    reports = models.PositiveIntegerField(default=0)
    new_versions = models.PositiveIntegerField(default=0)
    message = models.TextField(blank=True)

    class Meta:
        ordering = ["-started_at"]
