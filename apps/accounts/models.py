from django.contrib.auth.models import AbstractUser
from django.core.exceptions import ValidationError
from django.db import models


class User(AbstractUser):
    """
    Two kinds of users:

    - Employees see every brand and can manage brands, products and users.
    - Clients see reports for exactly one brand, read-only.

    Superusers are always treated as employees.
    """

    class Role(models.TextChoices):
        EMPLOYEE = "employee", "Employee (all brands)"
        CLIENT = "client", "Client (one brand)"

    role = models.CharField(max_length=16, choices=Role.choices, default=Role.CLIENT)
    brand = models.ForeignKey(
        "catalog.Brand",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="client_users",
        help_text="The only brand a client can see.",
    )

    class Meta:
        ordering = ["username"]

    def clean(self):
        super().clean()
        if self.is_superuser:
            self.role = self.Role.EMPLOYEE
        if self.role == self.Role.CLIENT and not self.brand_id:
            raise ValidationError({"brand": "Client users must be assigned a brand."})
        if self.role == self.Role.EMPLOYEE:
            self.brand = None

    def save(self, *args, **kwargs):
        if self.is_superuser:
            self.role = self.Role.EMPLOYEE
            self.brand = None
        super().save(*args, **kwargs)

    @property
    def is_employee(self) -> bool:
        return self.is_superuser or self.role == self.Role.EMPLOYEE

    def visible_brands(self):
        from apps.catalog.models import Brand

        if self.is_employee:
            return Brand.objects.all()
        return Brand.objects.filter(pk=self.brand_id)

    def can_view_brand(self, brand) -> bool:
        return self.is_employee or (brand is not None and brand.pk == self.brand_id)
