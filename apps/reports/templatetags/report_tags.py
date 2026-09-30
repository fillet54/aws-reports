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
