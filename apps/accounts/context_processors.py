from django.conf import settings

from apps.catalog.models import Marketplace


def navigation(request):
    """Brands and stores for the top navigation bar."""
    user = getattr(request, "user", None)
    if user is None or not user.is_authenticated:
        return {}
    return {
        "nav_brands": list(user.visible_brands()),
        "nav_marketplaces": list(Marketplace.objects.all()),
        "report_time_zone": settings.REPORT_TIME_ZONE,
    }
