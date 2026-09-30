from django.contrib import admin

from .models import RawReport, SyncRun


@admin.register(RawReport)
class RawReportAdmin(admin.ModelAdmin):
    list_display = ("brand", "source", "report_type", "fetched_at", "row_count", "order_count", "new_versions")
    list_filter = ("brand", "source")
    readonly_fields = [f.name for f in RawReport._meta.fields]


@admin.register(SyncRun)
class SyncRunAdmin(admin.ModelAdmin):
    list_display = ("brand", "client", "started_at", "status", "reports", "new_versions")
    list_filter = ("brand", "status")
