from decimal import Decimal

from django.core.validators import RegexValidator
from django.db import models
from django.urls import reverse

from . import crypto


class Marketplace(models.Model):
    """An Amazon store front (e.g. Amazon.com for the US, Amazon.ca for Canada)."""

    code = models.CharField(max_length=8, unique=True, help_text="Short code, e.g. US or CA.")
    name = models.CharField(max_length=64)
    amazon_marketplace_id = models.CharField(max_length=32, unique=True)
    sales_channel = models.CharField(
        max_length=64,
        unique=True,
        help_text="Value of the 'sales-channel' column in Amazon order reports, e.g. Amazon.com.",
    )
    currency = models.CharField(max_length=3)
    usd_rate = models.DecimalField(
        "USD exchange rate",
        max_digits=10,
        decimal_places=6,
        default=Decimal("1"),
        help_text="Multiply amounts in this marketplace's currency by this to get USD. "
        "Used when combining stores.",
    )
    sort_order = models.PositiveSmallIntegerField(default=0)

    class Meta:
        ordering = ["sort_order", "code"]

    def __str__(self) -> str:
        return f"{self.name} ({self.code})"


class Brand(models.Model):
    """A client brand. Each brand is its own Amazon seller account."""

    name = models.CharField(max_length=128)
    slug = models.SlugField(max_length=64, unique=True, help_text="Used in URLs.")
    marketplaces = models.ManyToManyField(Marketplace, related_name="brands", blank=True)

    selling_partner_id = models.CharField(
        "Selling partner ID",
        max_length=64,
        blank=True,
        help_text="Merchant token of the seller account (returned when the app is authorized).",
    )
    refresh_token_encrypted = models.TextField(blank=True, editable=False)
    sync_enabled = models.BooleanField(
        default=True, help_text="Include this brand in scheduled Amazon syncs."
    )
    last_synced_at = models.DateTimeField(null=True, blank=True, editable=False)

    # Amazon Ads API (sponsored ads and DSP) is authorized separately from SP-API.
    ads_refresh_token_encrypted = models.TextField(blank=True, editable=False)
    ads_profiles = models.JSONField(
        default=dict, blank=True, editable=False,
        help_text="Advertising profile ID per store code, discovered from the Ads API.",
    )
    dsp_advertisers = models.CharField(
        "DSP advertiser IDs",
        max_length=255,
        blank=True,
        help_text="Per store, e.g. US=5823901234, CA=6620194411. Leave blank if the brand doesn't run DSP.",
    )
    performance_synced_at = models.DateTimeField(null=True, blank=True, editable=False)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

    def get_absolute_url(self) -> str:
        return reverse("reports:brand_dashboard", kwargs={"slug": self.slug})

    # Refresh token helpers: stored encrypted, never rendered back to the browser.
    @property
    def has_refresh_token(self) -> bool:
        return bool(self.refresh_token_encrypted)

    def set_refresh_token(self, token: str) -> None:
        self.refresh_token_encrypted = crypto.encrypt(token) if token else ""

    def get_refresh_token(self) -> str:
        if not self.refresh_token_encrypted:
            return ""
        return crypto.decrypt(self.refresh_token_encrypted)

    @property
    def has_ads_token(self) -> bool:
        return bool(self.ads_refresh_token_encrypted)

    def set_ads_refresh_token(self, token: str) -> None:
        self.ads_refresh_token_encrypted = crypto.encrypt(token) if token else ""

    def get_ads_refresh_token(self) -> str:
        if not self.ads_refresh_token_encrypted:
            return ""
        return crypto.decrypt(self.ads_refresh_token_encrypted)

    def dsp_advertiser_map(self) -> dict[str, str]:
        """Parse dsp_advertisers ("US=123, CA=456"; a bare ID means US) into {store code: ID}."""
        result = {}
        for part in self.dsp_advertisers.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            code, sep, advertiser = part.partition("=")
            if not sep:
                code, advertiser = "US", code
            result[code.strip().upper()] = advertiser.strip()
        return result


asin_validator = RegexValidator(
    r"^[A-Z0-9]{10}$", "An ASIN is 10 characters: capital letters and digits."
)


class Product(models.Model):
    """
    Per-brand metadata for an ASIN. Products are created automatically when an
    ASIN first shows up in order data; the display title can then be edited.
    """

    brand = models.ForeignKey(Brand, on_delete=models.CASCADE, related_name="products")
    asin = models.CharField("ASIN", max_length=10, validators=[asin_validator])
    title = models.CharField(
        max_length=255, blank=True, help_text="Short name shown in reports."
    )
    amazon_title = models.CharField(
        max_length=500, blank=True, editable=False, help_text="Latest product name seen from Amazon."
    )
    sku = models.CharField("SKU", max_length=64, blank=True)
    category = models.CharField(max_length=128, blank=True)
    unit_cost = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    launch_date = models.DateField(null=True, blank=True)
    notes = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["title", "asin"]
        constraints = [
            models.UniqueConstraint(fields=["brand", "asin"], name="unique_product_asin_per_brand")
        ]

    def __str__(self) -> str:
        return f"{self.display_title} ({self.asin})"

    @property
    def display_title(self) -> str:
        return self.title or self.amazon_title or self.asin
