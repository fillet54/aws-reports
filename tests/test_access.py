import pytest
from django.urls import reverse

from apps.accounts.models import User
from apps.reports.models import SharedReport


def test_anonymous_users_are_sent_to_login(client, acme):
    response = client.get(reverse("reports:brand_dashboard", args=["acme"]))
    assert response.status_code == 302
    assert reverse("login") in response["Location"]


def test_client_lands_on_own_brand(brand_client):
    response = brand_client.get(reverse("home"))
    assert response["Location"] == reverse("reports:brand_dashboard", args=["acme"])


def test_client_sees_own_brand(brand_client):
    assert brand_client.get(reverse("reports:brand_dashboard", args=["acme"])).status_code == 200
    assert brand_client.get(reverse("reports:brand_report", args=["acme"])).status_code == 200
    assert brand_client.get(reverse("catalog:product_list", args=["acme"])).status_code == 200


def test_client_cannot_see_other_brand(brand_client, globex):
    for name in ("reports:brand_dashboard", "reports:brand_report", "catalog:product_list"):
        assert brand_client.get(reverse(name, args=["globex"])).status_code == 404


def test_client_cannot_see_all_brands_overview(brand_client):
    assert brand_client.get(reverse("reports:overview")).status_code == 403
    assert brand_client.get(reverse("reports:overview_report")).status_code == 403


@pytest.mark.parametrize(
    "url",
    [
        reverse("catalog:brand_list"),
        reverse("catalog:brand_create"),
        reverse("accounts:user_list"),
        reverse("accounts:user_create"),
        reverse("catalog:product_create", args=["acme"]),
    ],
)
def test_client_cannot_use_management_pages(brand_client, url):
    assert brand_client.get(url).status_code == 403


def test_client_cannot_edit_products(brand_client, acme):
    acme.products.create(asin="B000000001", title="Serum")
    url = reverse("catalog:product_edit", args=["acme", "B000000001"])
    assert brand_client.post(url, {"title": "Hacked"}).status_code == 403


def test_client_cannot_open_other_brands_shared_link(brand_client, globex, employee):
    shared = SharedReport.objects.create(brand=globex, period="week", start="2026-09-07", created_by=employee)
    assert brand_client.get(shared.get_absolute_url()).status_code == 404


def test_client_cannot_open_all_brands_shared_link(brand_client, employee):
    shared = SharedReport.objects.create(brand=None, period="week", start="2026-09-07", created_by=employee)
    assert brand_client.get(shared.get_absolute_url()).status_code == 404


def test_client_cannot_share_other_brand(brand_client, globex):
    response = brand_client.post(
        reverse("reports:share_create"), {"brand": "globex", "period": "week", "start": "2026-09-07"}
    )
    assert response.status_code == 404
    assert not SharedReport.objects.exists()


def test_employee_sees_everything(employee_client, acme, globex):
    for url in (
        reverse("home"),
        reverse("reports:overview"),
        reverse("reports:overview_report"),
        reverse("reports:overview_report") + "?period=month&store=CA",
        reverse("reports:brand_dashboard", args=["globex"]),
        reverse("catalog:brand_list"),
        reverse("catalog:brand_edit", args=["acme"]),
        reverse("accounts:user_list"),
        reverse("reports:shared_list"),
    ):
        assert employee_client.get(url, follow=True).status_code == 200, url


def test_employee_creates_client_user(employee_client, acme):
    response = employee_client.post(reverse("accounts:user_create"), {
        "username": "newclient", "role": "client", "brand": acme.pk,
        "password1": "a-long-password-123", "password2": "a-long-password-123",
    })
    assert response.status_code == 302
    user = User.objects.get(username="newclient")
    assert user.role == "client" and user.brand == acme


def test_client_user_requires_brand(employee_client):
    response = employee_client.post(reverse("accounts:user_create"), {
        "username": "nobrand", "role": "client",
        "password1": "a-long-password-123", "password2": "a-long-password-123",
    })
    assert response.status_code == 200
    assert not User.objects.filter(username="nobrand").exists()


def test_employee_role_clears_brand(employee_client, acme, client_user):
    employee_client.post(reverse("accounts:user_edit", args=[client_user.pk]), {
        "username": "cli", "role": "employee", "brand": acme.pk, "is_active": "on",
    })
    client_user.refresh_from_db()
    assert client_user.role == "employee" and client_user.brand is None


def test_employee_cannot_change_superuser(employee_client):
    admin = User.objects.create_superuser("root", password="pw-root-12345")
    assert employee_client.get(reverse("accounts:user_set_password", args=[admin.pk])).status_code == 403
    response = employee_client.post(
        reverse("accounts:user_set_password", args=[admin.pk]),
        {"password1": "taken-over-123", "password2": "taken-over-123"},
    )
    assert response.status_code == 403
    admin.refresh_from_db()
    assert admin.check_password("pw-root-12345")
    assert employee_client.get(reverse("accounts:user_edit", args=[admin.pk])).status_code == 403
