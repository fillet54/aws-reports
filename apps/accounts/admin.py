from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin

from .models import User


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    list_display = ("username", "email", "role", "brand", "is_active", "is_superuser")
    list_filter = ("role", "brand", "is_active", "is_superuser")
    fieldsets = BaseUserAdmin.fieldsets + (("Access", {"fields": ("role", "brand")}),)
    add_fieldsets = BaseUserAdmin.add_fieldsets + (("Access", {"fields": ("role", "brand")}),)
