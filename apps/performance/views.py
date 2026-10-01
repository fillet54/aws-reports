from datetime import timedelta

from django.shortcuts import render

from apps.accounts.access import employee_required
from apps.reports.periods import report_today
from apps.reports.views import _marketplace_from, _scope

from . import dashboard


@employee_required
def overview_performance(request):
    return _performance(request, slug=None)


def brand_performance(request, slug: str):
    return _performance(request, slug=slug)


def _performance(request, slug):
    scope = _scope(request, slug, marketplace=_marketplace_from(request))
    yesterday = report_today() - timedelta(days=1)
    data = dashboard.build(scope, yesterday)
    brand = None if scope.all_brands else scope.brands[0]
    synced = [b.performance_synced_at for b in scope.brands if b.performance_synced_at]
    return render(request, "performance/dashboard.html", {
        "scope": scope,
        "brand": brand,
        "slug": slug,
        "current_store": scope.marketplace,
        "currency": scope.currency,
        "yesterday": yesterday,
        "performance_synced": max(synced) if synced else None,
        **data,
    })
