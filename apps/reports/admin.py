from django.contrib import admin

from .models import SharedReport


@admin.register(SharedReport)
class SharedReportAdmin(admin.ModelAdmin):
    list_display = ("token", "brand", "marketplace", "period", "start", "data_cutoff", "created_by")
    list_filter = ("brand", "period")
