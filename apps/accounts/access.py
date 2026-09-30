"""
Brand-level access control. Every view that touches brand data goes through
these helpers so the "client sees one brand" rule lives in one place.
"""

from functools import wraps

from django.core.exceptions import PermissionDenied
from django.http import Http404
from django.shortcuts import get_object_or_404

from apps.catalog.models import Brand


def employee_required(view):
    """Allow only employees (and superusers). Clients get a 403."""

    @wraps(view)
    def wrapper(request, *args, **kwargs):
        if not request.user.is_authenticated or not request.user.is_employee:
            raise PermissionDenied
        return view(request, *args, **kwargs)

    return wrapper


def get_brand_for_user(user, slug: str) -> Brand:
    """
    Return the brand if the user may see it. Brands the user can't see are
    reported as missing (404) so clients can't probe for other brand names.
    """
    brand = get_object_or_404(Brand, slug=slug)
    if not user.can_view_brand(brand):
        raise Http404
    return brand
