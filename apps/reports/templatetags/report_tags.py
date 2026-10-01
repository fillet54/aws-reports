from decimal import Decimal

from django import template
from django.contrib.humanize.templatetags.humanize import intcomma

register = template.Library()

SYMBOLS = {"USD": "$", "CAD": "CA$"}


@register.filter
def money(value, currency="USD"):
    if value is None or value == "":
        return "—"
    amount = Decimal(value).quantize(Decimal("0.01"))
    sign = "-" if amount < 0 else ""
    return f"{sign}{SYMBOLS.get(currency, currency + ' ')}{intcomma(f'{abs(amount):.2f}')}"


@register.filter
def money0(value, currency="USD"):
    """Whole-unit money for headline numbers."""
    if value is None or value == "":
        return "—"
    amount = round(Decimal(value))
    sign = "-" if amount < 0 else ""
    return f"{sign}{SYMBOLS.get(currency, currency + ' ')}{intcomma(abs(amount))}"


@register.inclusion_tag("reports/_change.html")
def change(value, small=False):
    return {"value": value, "small": small}


@register.simple_tag
def metric(value, fmt, currency="USD"):
    """Format a performance dashboard value."""
    if value is None:
        return "—"
    if fmt == "money":
        return money0(value, currency)
    if fmt == "pct":
        return f"{Decimal(value):.1f}%"
    if fmt == "decimal":
        return f"{Decimal(value):,.0f}" if Decimal(value) == Decimal(value).to_integral() else f"{Decimal(value):,.1f}"
    return intcomma(int(value))


@register.inclusion_tag("performance/_change.html")
def perf_change(cell, tone):
    """Change vs last year, colored by whether up is good for this metric."""
    good = None
    if cell.change is not None and tone != "neutral":
        good = (cell.change >= 0) == (tone == "up")
    return {"cell": cell, "good": good}
