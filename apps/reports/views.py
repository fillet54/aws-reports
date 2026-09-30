from __future__ import annotations

from datetime import date, timedelta

from django.contrib import messages
from django.http import Http404
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from apps.accounts.access import employee_required, get_brand_for_user
from apps.catalog.models import Brand, Marketplace
from apps.sales import metrics
from apps.sales.metrics import Scope

from .models import SharedReport
from .periods import KINDS, WEEK, Period, local_datetime, pct_change, report_today, shift_year


# ---------------------------------------------------------------------------
# Scope helpers
# ---------------------------------------------------------------------------


def _marketplace_from(request) -> Marketplace | None:
    code = (request.GET.get("store") or request.POST.get("store") or "").upper()
    if not code:
        return None
    return Marketplace.objects.filter(code=code).first()


def _scope(request, slug: str | None, as_of=None, marketplace=None) -> Scope:
    if slug is None:
        if not request.user.is_employee:
            raise Http404
        return Scope(brands=tuple(Brand.objects.all()), marketplace=marketplace, as_of=as_of, all_brands=True)
    brand = get_brand_for_user(request.user, slug)
    return Scope(brands=(brand,), marketplace=marketplace, as_of=as_of)


def _compare(current: dict, previous: dict) -> dict:
    return {
        **current,
        "prev": previous,
        "sales_change": pct_change(current["sales"], previous["sales"]),
        "units_change": pct_change(current["units"], previous["units"]),
        "orders_change": pct_change(current["orders"], previous["orders"]),
    }


def _last_synced(scope: Scope):
    times = [b.last_synced_at for b in scope.brands if b.last_synced_at]
    return max(times) if times else None


def _base_context(request, scope: Scope, slug: str | None) -> dict:
    return {
        "scope": scope,
        "brand": scope.brands[0] if not scope.all_brands and scope.brands else None,
        "slug": slug,
        "current_store": scope.marketplace,
        "currency": scope.currency,
        "last_synced": _last_synced(scope),
    }


# ---------------------------------------------------------------------------
# Dashboards
# ---------------------------------------------------------------------------


def home(request):
    user = request.user
    if user.is_employee:
        return redirect("reports:overview")
    if user.brand_id:
        return redirect("reports:brand_dashboard", slug=user.brand.slug)
    raise Http404("Your account isn't linked to a brand yet.")


@employee_required
def overview(request):
    return _dashboard(request, slug=None)


def brand_dashboard(request, slug: str):
    return _dashboard(request, slug=slug)


def _dashboard(request, slug: str | None):
    scope = _scope(request, slug, marketplace=_marketplace_from(request))
    today = report_today()

    now = timezone.now()

    def span(start: date, ly_start: date) -> dict:
        # Compare with last year up to the same time of day, since today isn't over.
        offset = start - ly_start
        ly_end = today - offset
        return _compare(
            metrics.totals(scope, start, today),
            metrics.totals(scope, ly_start, ly_end, until=now - offset),
        )

    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)
    kpis = [
        ("Week to date", week_start, span(week_start, week_start - timedelta(weeks=52))),
        ("Month to date", month_start, span(month_start, shift_year(month_start, -1))),
        ("Year to date", year_start, span(year_start, shift_year(year_start, -1))),
    ]

    # Monthly chart: this year vs last year.
    last_year_start = shift_year(year_start, -1)
    months = metrics.by_month(scope, last_year_start, date(today.year, 12, 31))
    chart = {
        "labels": [date(2000, m, 1).strftime("%b") for m in range(1, 13)],
        "current": [float(months.get(date(today.year, m, 1), {}).get("sales", 0)) if m <= today.month else None for m in range(1, 13)],
        "previous": [float(months.get(date(today.year - 1, m, 1), {}).get("sales", 0)) for m in range(1, 13)],
        "current_label": str(today.year),
        "previous_label": str(today.year - 1),
        "currency": scope.currency,
    }

    # Month-to-date breakdowns with last-year comparison.
    ly_month_start = shift_year(month_start, -1)
    ly_today = ly_month_start + (today - month_start)
    breakdowns = _breakdowns(scope, month_start, today, ly_month_start, ly_today)

    context = _base_context(request, scope, slug) | {
        "kpis": kpis,
        "chart": chart,
        "today": today,
        "breakdown_label": f"Month to date ({month_start:%b} 1 – {today:%b} {today.day})",
        **breakdowns,
        "top_products": breakdowns["products"][:10],
    }
    return render(request, "reports/dashboard.html", context)


def _breakdowns(scope: Scope, start: date, end: date, ly_start: date, ly_end: date) -> dict:
    brand_rows = []
    if len(scope.brands) > 1:
        current, previous = metrics.by_brand(scope, start, end), metrics.by_brand(scope, ly_start, ly_end)
        empty = {"sales": metrics.ZERO, "units": 0, "orders": 0}
        for b in scope.brands:
            brand_rows.append({**_compare(current.get(b.pk, empty), previous.get(b.pk, empty)), "brand": b})
        brand_rows.sort(key=lambda r: r["sales"], reverse=True)

    store_rows = []
    if scope.marketplace is None:
        current, previous = metrics.by_marketplace(scope, start, end), metrics.by_marketplace(scope, ly_start, ly_end)
        for pk, row in current.items():
            store_rows.append(_compare(row, previous[pk]))

    current, previous = metrics.by_product(scope, start, end), metrics.by_product(scope, ly_start, ly_end)
    brands = {b.pk: b for b in scope.brands}
    empty = {"sales": metrics.ZERO, "units": 0, "orders": 0}
    product_rows = []
    for key, row in current.items():
        product_rows.append({**_compare(row, previous.get(key, empty)), "brand_obj": brands.get(key[0])})
    product_rows.sort(key=lambda r: r["sales"], reverse=True)

    return {"brand_rows": brand_rows, "store_rows": store_rows, "products": product_rows}


# ---------------------------------------------------------------------------
# Weekly / monthly reports
# ---------------------------------------------------------------------------


def _period_from(request) -> Period:
    kind = request.GET.get("period", WEEK)
    if kind not in KINDS:
        kind = WEEK
    try:
        day = date.fromisoformat(request.GET.get("date", ""))
    except ValueError:
        day = report_today()
        if kind == WEEK:
            # Default to the last complete week: what you'd send around on Monday.
            day -= timedelta(days=7)
    return Period.containing(kind, day)


def period_report_context(scope: Scope, period: Period, today: date, now=None) -> dict:
    """
    Numbers for one week or month. If the period is still in progress, the
    comparison periods are cut at the same point in time (`now`, shifted).
    """
    end = period.clipped_end(today)
    partial = end < period.end
    days = (end - period.start).days

    prev_period = period.previous()
    ly_period = period.last_year()
    prev_end = min(prev_period.start + timedelta(days=days), prev_period.end)
    ly_end = min(ly_period.start + timedelta(days=days), ly_period.end)

    until_prev = until_ly = None
    if partial and now is not None:
        until_prev = now - (period.start - prev_period.start)
        until_ly = now - (period.start - ly_period.start)

    current = metrics.totals(scope, period.start, end)
    summary = {
        "current": current,
        "vs_prev": _compare(current, metrics.totals(scope, prev_period.start, prev_end, until=until_prev)),
        "vs_ly": _compare(current, metrics.totals(scope, ly_period.start, ly_end, until=until_ly)),
        "aov": (current["sales"] / current["orders"]) if current["orders"] else None,
    }

    daily = metrics.by_day(scope, period.start, end)
    daily_ly = metrics.by_day(scope, ly_period.start, ly_period.end)
    chart = {
        "labels": [f"{d:%a} {d.day}" if period.kind == WEEK else str(d.day) for d in period.days()],
        "current": [float(daily.get(d, {}).get("sales", 0)) if d <= end else None for d in period.days()],
        "previous": [float(daily_ly.get(d, {}).get("sales", 0)) for d in ly_period.days()][: len(period.days())],
        "current_label": period.label,
        "previous_label": "Last year",
        "currency": scope.currency,
    }

    return {
        "period": period,
        "partial": partial,
        "through": end,
        "prev_period": prev_period,
        "ly_period": ly_period,
        "summary": summary,
        "chart": chart,
        **_breakdowns(scope, period.start, end, ly_period.start, ly_end),
    }


@employee_required
def overview_report(request):
    return _report(request, slug=None)


def brand_report(request, slug: str):
    return _report(request, slug=slug)


def _report(request, slug: str | None):
    scope = _scope(request, slug, marketplace=_marketplace_from(request))
    period = _period_from(request)
    context = _base_context(request, scope, slug) | period_report_context(
        scope, period, report_today(), now=timezone.now()
    )
    context["today"] = report_today()
    return render(request, "reports/report.html", context)


# ---------------------------------------------------------------------------
# Shared (frozen) reports
# ---------------------------------------------------------------------------


@require_POST
def share_create(request):
    slug = request.POST.get("brand") or None
    scope = _scope(request, slug)  # access check
    try:
        period = Period.containing(request.POST.get("period", WEEK), date.fromisoformat(request.POST["start"]))
    except (KeyError, ValueError):
        raise Http404
    if period.kind not in KINDS:
        raise Http404
    shared = SharedReport.objects.create(
        brand=None if scope.all_brands else scope.brands[0],
        marketplace=_marketplace_from(request),
        period=period.kind,
        start=period.start,
        created_by=request.user,
    )
    messages.success(request, "Share link created. The numbers on it won't change until someone refreshes it.")
    return redirect(shared)


def _get_shared(request, token: str) -> SharedReport:
    shared = get_object_or_404(SharedReport.objects.select_related("brand", "marketplace"), token=token)
    if not shared.can_view(request.user):
        raise Http404
    return shared


def shared_report(request, token: str):
    shared = _get_shared(request, token)
    brands = (shared.brand,) if shared.brand else tuple(Brand.objects.all())
    scope = Scope(brands=brands, marketplace=shared.marketplace, as_of=shared.data_cutoff, all_brands=shared.brand is None)
    period = Period.containing(shared.period, shared.start)
    today = report_today()
    context = {
        "scope": scope,
        "brand": shared.brand,
        "slug": shared.brand.slug if shared.brand else None,
        "current_store": shared.marketplace,
        "currency": scope.currency,
        "shared": shared,
        "share_url": request.build_absolute_uri(shared.get_absolute_url()),
        "today": today,
        **period_report_context(
            scope, period, min(today, local_datetime(shared.data_cutoff).date()), now=shared.data_cutoff
        ),
    }
    return render(request, "reports/shared.html", context)


@require_POST
def shared_refresh(request, token: str):
    shared = _get_shared(request, token)
    shared.data_cutoff = timezone.now()
    shared.refreshed_at = shared.data_cutoff
    shared.refreshed_by = request.user
    shared.save(update_fields=["data_cutoff", "refreshed_at", "refreshed_by"])
    messages.success(request, "Report refreshed with the latest data.")
    return redirect(shared)


def shared_list(request):
    qs = SharedReport.objects.select_related("brand", "marketplace", "created_by")
    if not request.user.is_employee:
        qs = qs.filter(brand_id=request.user.brand_id)
    return render(request, "reports/shared_list.html", {"shared_reports": qs[:200]})
