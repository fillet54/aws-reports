from django import forms
from django.contrib.auth.forms import (
    AdminPasswordChangeForm,
    AuthenticationForm,
    PasswordChangeForm,
    UserCreationForm,
)

from apps.catalog.models import Brand

from .forms_base import StyledFormMixin
from .models import User


class LoginForm(StyledFormMixin, AuthenticationForm):
    pass


class OwnPasswordChangeForm(StyledFormMixin, PasswordChangeForm):
    pass


class SetUserPasswordForm(StyledFormMixin, AdminPasswordChangeForm):
    pass


class _RoleBrandMixin:
    """Client users need a brand; employees must not have one."""

    def clean(self):
        cleaned = super().clean()
        role, brand = cleaned.get("role"), cleaned.get("brand")
        if role == User.Role.CLIENT and not brand:
            self.add_error("brand", "Pick the brand this client can see.")
        if role == User.Role.EMPLOYEE:
            cleaned["brand"] = None
        return cleaned


class UserCreateForm(StyledFormMixin, _RoleBrandMixin, UserCreationForm):
    class Meta:
        model = User
        fields = ["username", "first_name", "last_name", "email", "role", "brand"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["brand"].queryset = Brand.objects.all()
        self.fields["brand"].required = False


class UserEditForm(StyledFormMixin, _RoleBrandMixin, forms.ModelForm):
    class Meta:
        model = User
        fields = ["username", "first_name", "last_name", "email", "role", "brand", "is_active"]
        help_texts = {"is_active": "Inactive users can't log in."}

    def __init__(self, *args, editing_self=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["brand"].required = False
        if editing_self:
            # Don't let people lock themselves out.
            for name in ("role", "is_active"):
                self.fields[name].disabled = True
        if self.instance.is_superuser:
            self.fields["role"].disabled = True
            self.fields["role"].help_text = "Superusers are always employees."
