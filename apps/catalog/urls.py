from django.urls import path

from . import views

app_name = "catalog"

urlpatterns = [
    path("manage/brands/", views.brand_list, name="brand_list"),
    path("manage/brands/new/", views.brand_create, name="brand_create"),
    path("manage/brands/<slug:slug>/", views.brand_edit, name="brand_edit"),
    path("manage/brands/<slug:slug>/sync/", views.brand_sync, name="brand_sync"),
    path("manage/brands/<slug:slug>/upload/", views.brand_upload, name="brand_upload"),
    path("b/<slug:slug>/products/", views.product_list, name="product_list"),
    path("b/<slug:slug>/products/new/", views.product_create, name="product_create"),
    path("b/<slug:slug>/products/<str:asin>/", views.product_edit, name="product_edit"),
    path("b/<slug:slug>/products/<str:asin>/delete/", views.product_delete, name="product_delete"),
]
