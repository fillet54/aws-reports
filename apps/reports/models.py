import secrets

from django.conf import settings
from django.db import models
from django.urls import reverse
from django.utils import timezone


def new_token() -> str:
    return secrets.token_urlsafe(12)


class SharedReport(models.Model):
    """
    A link to a weekly or monthly report whose numbers are frozen at
    `data_cutoff`: only data fetched from Amazon up to that moment is used, so
    everyone who opens the link sees the same figures until someone presses
    "Refresh data".
    """

    token = models.CharField(max_length=32, unique=True, default=new_token, editable=False)
    brand = models.ForeignKey(
        "catalog.Brand",
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name="shared_reports",
        help_text="Empty means all brands (employees only).",
    )
    marketplace = models.ForeignKey(
        "catalog.Marketplace", on_delete=models.CASCADE, null=True, blank=True, related_name="+"
    )
    period = models.CharField(max_length=8)
    start = models.DateField()
    data_cutoff = models.DateTimeField(default=timezone.now)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="+"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    refreshed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="+"
    )
    refreshed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        who = self.brand.name if self.brand else "All brands"
        return f"{who} {self.period} {self.start}"

    def get_absolute_url(self) -> str:
        return reverse("reports:shared", kwargs={"token": self.token})

    def can_view(self, user) -> bool:
        if not user.is_authenticated:
            return False
        if self.brand is None:
            return user.is_employee
        return user.can_view_brand(self.brand)
