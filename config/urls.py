from django.contrib import admin
from django.contrib.auth import views as auth_views
from django.urls import include, path

from apps.accounts import views as account_views
from apps.reports import views as report_views

urlpatterns = [
    path("", report_views.home, name="home"),
    path("login/", account_views.LoginView.as_view(), name="login"),
    path("logout/", auth_views.LogoutView.as_view(), name="logout"),
    path("account/password/", account_views.PasswordChangeView.as_view(), name="password_change"),
    path("manage/users/", include("apps.accounts.urls")),
    path("", include("apps.catalog.urls")),
    path("", include("apps.reports.urls")),
    path("admin/", admin.site.urls),
]
