from django.urls import path

from . import views

app_name = "accounts"

urlpatterns = [
    path("", views.user_list, name="user_list"),
    path("new/", views.user_create, name="user_create"),
    path("<int:pk>/", views.user_edit, name="user_edit"),
    path("<int:pk>/password/", views.user_set_password, name="user_set_password"),
]
