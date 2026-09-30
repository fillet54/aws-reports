from django.contrib import admin

from .models import Brand, Marketplace, Product


@admin.register(Marketplace)
class MarketplaceAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "sales_channel", "currency", "usd_rate", "amazon_marketplace_id")
    list_editable = ("usd_rate",)


@admin.register(Brand)
class BrandAdmin(admin.ModelAdmin):
    list_display = ("name", "slug", "selling_partner_id", "sync_enabled", "last_synced_at")
    prepopulated_fields = {"slug": ("name",)}
    filter_horizontal = ("marketplaces",)


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("asin", "title", "brand", "sku", "category")
    list_filter = ("brand",)
    search_fields = ("asin", "title", "amazon_title", "sku")
