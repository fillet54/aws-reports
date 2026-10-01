from django.contrib import admin

from .models import DailyAds, DailySubscriptions, DailyTraffic, DataPull


@admin.register(DataPull)
class DataPullAdmin(admin.ModelAdmin):
    list_display = ("brand", "marketplace", "kind", "source", "start_date", "end_date", "fetched_at", "days")
    list_filter = ("brand", "kind", "marketplace")


@admin.register(DailyTraffic)
class DailyTrafficAdmin(admin.ModelAdmin):
    list_display = ("brand", "marketplace", "date", "sessions", "units_ordered", "ordered_product_sales", "average_offer_count")
    list_filter = ("brand", "marketplace")
    date_hierarchy = "date"


@admin.register(DailyAds)
class DailyAdsAdmin(admin.ModelAdmin):
    list_display = ("brand", "marketplace", "date", "ad_product", "cost", "sales", "clicks")
    list_filter = ("brand", "marketplace", "ad_product")
    date_hierarchy = "date"


@admin.register(DailySubscriptions)
class DailySubscriptionsAdmin(admin.ModelAdmin):
    list_display = ("brand", "marketplace", "date", "active_subscriptions", "shipped_units", "revenue")
    list_filter = ("brand", "marketplace")
    date_hierarchy = "date"
