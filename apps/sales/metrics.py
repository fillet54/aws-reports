"""
Report queries over the order tables.

A Scope says which data to look at: one or more brands, one store or all of
them, and optionally a data cutoff ("as known at"). All report numbers come
from functions in this module so the revenue definition lives in one place.

Revenue: sum of item-price (the line total Amazon reports, before tax), for
lines that have a price. Pending orders have no price yet, so they're counted
once Amazon prices them. When stores with different currencies are combined,
amounts are converted to USD with each marketplace's usd_rate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal

from django.db.models import Count, DecimalField, ExpressionWrapper, F, Max, OuterRef, QuerySet, Subquery, Sum
from django.db.models.functions import Coalesce, TruncMonth

from apps.catalog.models import Brand, Marketplace, Product

from .models import OrderLine, OrderVersion

ZERO = Decimal("0")
MONEY = DecimalField(max_digits=16, decimal_places=2)


@dataclass(frozen=True)
class Scope:
    brands: tuple[Brand, ...]
    marketplace: Marketplace | None = None
    as_of: datetime | None = None
    all_brands: bool = field(default=False)

    @property
    def brand_ids(self) -> list[int]:
        return [b.pk for b in self.brands]

    @property
    def currency(self) -> str:
        return self.marketplace.currency if self.marketplace else "USD"

    @property
    def converts_currency(self) -> bool:
        return self.marketplace is None


def _versions_as_of(brand_ids: list[int], as_of: datetime):
    latest = (
        OrderVersion.objects.filter(
            brand=OuterRef("brand"),
            amazon_order_id=OuterRef("amazon_order_id"),
            fetched_at__lte=as_of,
        )
        .order_by("-fetched_at", "-id")
        .values("id")[:1]
    )
    return (
        OrderVersion.objects.filter(brand__in=brand_ids, fetched_at__lte=as_of)
        .annotate(latest_id=Subquery(latest))
        .filter(id=F("latest_id"))
        .values("id")
    )


def latest_fetch(brand_ids: list[int]) -> datetime | None:
    return OrderVersion.objects.filter(brand__in=brand_ids).aggregate(m=Max("fetched_at"))["m"]


def lines(scope: Scope) -> QuerySet[OrderLine]:
    qs = OrderLine.objects.filter(
        brand__in=scope.brand_ids, marketplace__isnull=False, item_price__isnull=False
    )
    if scope.marketplace is not None:
        qs = qs.filter(marketplace=scope.marketplace)

    newest = latest_fetch(scope.brand_ids)
    if scope.as_of is None or (newest is not None and scope.as_of >= newest):
        return qs.filter(version__is_current=True)
    return qs.filter(version_id__in=_versions_as_of(scope.brand_ids, scope.as_of))


def _sales_expr(scope: Scope):
    if scope.converts_currency:
        return Sum(ExpressionWrapper(F("item_price") * F("marketplace__usd_rate"), output_field=MONEY))
    return Sum("item_price")


def _aggregates(scope: Scope) -> dict:
    return {
        "sales": Coalesce(_sales_expr(scope), ZERO, output_field=MONEY),
        "units": Coalesce(Sum("quantity"), 0),
        "orders": Count("amazon_order_id", distinct=True),
    }


def _clean(row: dict) -> dict:
    row["sales"] = Decimal(row["sales"] or 0).quantize(Decimal("0.01"))
    row["units"] = int(row["units"] or 0)
    row["orders"] = int(row["orders"] or 0)
    return row


def totals(scope: Scope, start: date, end: date, until: datetime | None = None) -> dict:
    """
    Sales, units and orders for purchase days start..end inclusive. `until`
    also cuts off purchases after that moment, e.g. to compare "this week so
    far" with the same point in time last year.
    """
    qs = lines(scope).filter(purchase_day__gte=start, purchase_day__lte=end)
    if until is not None:
        qs = qs.filter(purchase_date__lte=until)
    return _clean(qs.aggregate(**_aggregates(scope)))


def grouped(scope: Scope, start: date, end: date, *fields: str) -> list[dict]:
    qs = (
        lines(scope)
        .filter(purchase_day__gte=start, purchase_day__lte=end)
        .values(*fields)
        .annotate(**_aggregates(scope))
        .order_by(*fields)
    )
    return [_clean(dict(r)) for r in qs]


def by_day(scope: Scope, start: date, end: date) -> dict[date, dict]:
    return {r["purchase_day"]: r for r in grouped(scope, start, end, "purchase_day")}


def by_month(scope: Scope, start: date, end: date) -> dict[date, dict]:
    qs = (
        lines(scope)
        .filter(purchase_day__gte=start, purchase_day__lte=end)
        .annotate(month=TruncMonth("purchase_day"))
        .values("month")
        .annotate(**_aggregates(scope))
        .order_by("month")
    )
    out = {}
    for r in qs:
        month = r["month"]
        if isinstance(month, datetime):
            month = month.date()
        out[month] = _clean(dict(r))
    return out


def by_brand(scope: Scope, start: date, end: date) -> dict[int, dict]:
    return {r["brand"]: r for r in grouped(scope, start, end, "brand")}


def by_marketplace(scope: Scope, start: date, end: date) -> dict[int, dict]:
    """Per store. Amounts stay in the store's own currency."""
    out = {}
    for m in Marketplace.objects.all():
        per_store = Scope(brands=scope.brands, marketplace=m, as_of=scope.as_of)
        row = totals(per_store, start, end)
        row["marketplace"] = m
        out[m.pk] = row
    return out


def by_product(scope: Scope, start: date, end: date) -> dict[tuple[int, str], dict]:
    rows = grouped(scope, start, end, "brand", "asin")
    products = {
        (p.brand_id, p.asin): p
        for p in Product.objects.filter(brand__in=scope.brand_ids, asin__in={r["asin"] for r in rows})
    }
    out = {}
    for r in rows:
        key = (r["brand"], r["asin"])
        r["product"] = products.get(key)
        out[key] = r
    return out
