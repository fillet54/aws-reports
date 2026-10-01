"""
The performance dashboard: one row per metric, one column per period, each
compared with the same period last year. All periods end yesterday, the
last complete day.

Sales and units come from order data (available immediately). Sessions,
conversion and offer count come from the Sales and Traffic report, ads from
the Ads API, and subscriptions from the Replenishment API. Those arrive a
day or more later, so each cell says whether its source covers the whole
period.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from django.db.models import Avg, DecimalField, ExpressionWrapper, F, Max, Sum
from django.db.models.functions import Coalesce

from apps.reports.periods import pct_change, shift_year
from apps.sales import metrics
from apps.sales.metrics import Scope

from .models import DailyAds, DailySubscriptions, DailyTraffic

ZERO = Decimal("0")
MONEY = DecimalField(max_digits=16, decimal_places=2)

# Cell states
COMPLETE, PARTIAL, MISSING = "complete", "partial", "missing"


@dataclass(frozen=True)
class Column:
    key: str
    label: str
    start: date
    end: date
    ly_start: date
    ly_end: date


def columns_for(yesterday: date) -> list[Column]:
    week_start = yesterday - timedelta(days=6)
    month_start = yesterday.replace(day=1)
    year_start = yesterday.replace(month=1, day=1)
    # Day and 7-day periods compare with the same weekdays last year (52
    # weeks back); month and year compare with the same calendar dates.
    shift = timedelta(weeks=52)
    return [
        Column("yesterday", "Yesterday", yesterday, yesterday, yesterday - shift, yesterday - shift),
        Column("last7", "Last 7 Days", week_start, yesterday, week_start - shift, yesterday - shift),
        Column("mtd", "Month-to-Date", month_start, yesterday, shift_year(month_start, -1), shift_year(yesterday, -1)),
        Column("ytd", "Year-to-Date", year_start, yesterday, shift_year(year_start, -1), shift_year(yesterday, -1)),
    ]


@dataclass(frozen=True)
class RowSpec:
    key: str
    label: str
    fmt: str            # money | int | pct | decimal
    source: str         # orders | ads | dsp | traffic | subscriptions
    tone: str = "up"    # up: higher is better; down: lower is better; neutral
    help: str = ""


ROWS = [
    RowSpec("sales", "Sales", "money", "orders", help="Item price of ordered items (excludes pending orders without a price)."),
    RowSpec("units", "Units", "int", "orders"),
    RowSpec("search_ad_sales", "Search Ad Sales", "money", "ads", help="Sponsored Products (7-day attribution) + Sponsored Brands + Sponsored Display (14-day)."),
    RowSpec("dsp_ad_sales", "DSP Ad Sales", "money", "dsp"),
    RowSpec("total_ad_sales", "Total Ad Sales", "money", "ads"),
    RowSpec("search_ad_cost", "Search Advertising Cost", "money", "ads", tone="neutral"),
    RowSpec("dsp_ad_cost", "DSP Advertising Cost", "money", "dsp", tone="neutral"),
    RowSpec("total_ad_cost", "Total Advertising Cost", "money", "ads", tone="neutral"),
    RowSpec("tacos", "TACOS", "pct", "ads", tone="down", help="Total advertising cost ÷ sales."),
    RowSpec("sessions", "Sessions", "int", "traffic"),
    RowSpec("conversion", "Conversion", "pct", "traffic", help="Units ordered ÷ sessions (Amazon's unit session percentage)."),
    RowSpec("offer_count", "Total Offer Count", "decimal", "traffic", help="Average number of offers listed in the period, summed across stores."),
    RowSpec("subscriptions", "S&S Sub Count", "int", "subscriptions", help="Active Subscribe & Save subscriptions at the end of the period."),
]


@dataclass
class Cell:
    value: Decimal | int | None
    ly_value: Decimal | int | None
    change: float | None
    state: str = COMPLETE


@dataclass
class Row:
    spec: RowSpec
    cells: list[Cell] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


def _filter(model, scope: Scope, start: date, end: date):
    qs = model.objects.filter(brand__in=scope.brand_ids, date__gte=start, date__lte=end)
    if scope.marketplace is not None:
        qs = qs.filter(marketplace=scope.marketplace)
    return qs


def _converted(field_name: str, scope: Scope):
    if scope.converts_currency:
        return Coalesce(
            Sum(ExpressionWrapper(F(field_name) * F("marketplace__usd_rate"), output_field=MONEY)),
            ZERO, output_field=MONEY,
        )
    return Coalesce(Sum(field_name), ZERO, output_field=MONEY)


def _ads(scope, start, end) -> dict:
    qs = _filter(DailyAds, scope, start, end)
    search = qs.filter(ad_product__in=DailyAds.SEARCH).aggregate(cost=_converted("cost", scope), sales=_converted("sales", scope))
    dsp = qs.filter(ad_product="DSP").aggregate(cost=_converted("cost", scope), sales=_converted("sales", scope))
    return {
        "search_ad_sales": search["sales"], "search_ad_cost": search["cost"],
        "dsp_ad_sales": dsp["sales"], "dsp_ad_cost": dsp["cost"],
        "total_ad_sales": search["sales"] + dsp["sales"],
        "total_ad_cost": search["cost"] + dsp["cost"],
    }


def _traffic(scope, start, end) -> dict:
    qs = _filter(DailyTraffic, scope, start, end)
    totals = qs.aggregate(sessions=Coalesce(Sum("sessions"), 0), units=Coalesce(Sum("units_ordered"), 0))
    # Average per brand and store over the period, then add those averages up.
    per_store = qs.values("brand", "marketplace").annotate(avg=Avg("average_offer_count"))
    offers = sum((Decimal(r["avg"]) for r in per_store if r["avg"] is not None), ZERO)
    sessions, units = totals["sessions"], totals["units"]
    return {
        "sessions": sessions,
        "conversion": (Decimal(units) / sessions * 100) if sessions else None,
        "offer_count": offers.quantize(Decimal("0.1")) if per_store else None,
    }


def _subscriptions(scope, start, end) -> dict:
    qs = _filter(DailySubscriptions, scope, start, end)
    # Count at the last day with data, per brand and store.
    latest = qs.values("brand", "marketplace").annotate(last=Max("date"))
    total, found = 0, False
    for row in latest:
        record = qs.filter(brand=row["brand"], marketplace=row["marketplace"], date=row["last"]).first()
        if record:
            total += record.active_subscriptions
            found = True
    return {"subscriptions": total if found else None}


def coverage(scope: Scope) -> dict[str, date | None]:
    """Last day with data, per source."""
    def last(model, **filters):
        qs = model.objects.filter(brand__in=scope.brand_ids, **filters)
        if scope.marketplace is not None:
            qs = qs.filter(marketplace=scope.marketplace)
        return qs.aggregate(m=Max("date"))["m"]

    return {
        "traffic": last(DailyTraffic),
        "ads": last(DailyAds, ad_product__in=DailyAds.SEARCH),
        "dsp": last(DailyAds, ad_product="DSP"),
        "subscriptions": last(DailySubscriptions),
    }


def _values(scope: Scope, start: date, end: date) -> dict:
    orders = metrics.totals(scope, start, end)
    values = {"sales": orders["sales"], "units": orders["units"]}
    values |= _ads(scope, start, end)
    values |= _traffic(scope, start, end)
    values |= _subscriptions(scope, start, end)
    values["tacos"] = (values["total_ad_cost"] / values["sales"] * 100) if values["sales"] else None
    return values


def _state(source: str, start: date, end: date, covered: dict) -> str:
    if source == "orders":
        return COMPLETE
    last = covered.get(source)
    if last is None or last < start:
        return MISSING
    return COMPLETE if last >= end else PARTIAL


def build(scope: Scope, yesterday: date) -> dict:
    cols = columns_for(yesterday)
    covered = coverage(scope)
    current = [_values(scope, c.start, c.end) for c in cols]
    previous = [_values(scope, c.ly_start, c.ly_end) for c in cols]

    rows = []
    for spec in ROWS:
        row = Row(spec)
        for col, now, before in zip(cols, current, previous):
            state = _state(spec.source, col.start, col.end, covered)
            # TACOS needs both sales and ad data.
            if spec.key == "tacos":
                state = _state("ads", col.start, col.end, covered)
            value = now.get(spec.key)
            ly_value = before.get(spec.key)
            if state == MISSING:
                value = None
            row.cells.append(Cell(value=value, ly_value=ly_value, change=pct_change(value, ly_value) if value is not None else None, state=state))
        rows.append(row)

    return {"columns": cols, "rows": rows, "coverage": covered}
