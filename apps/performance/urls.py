from django.urls import path

from . import views

app_name = "performance"

urlpatterns = [
    path("overview/performance/", views.overview_performance, name="overview"),
    path("b/<slug:slug>/performance/", views.brand_performance, name="brand"),
]
