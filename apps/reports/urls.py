from django.urls import path

from . import views

app_name = "reports"

urlpatterns = [
    path("overview/", views.overview, name="overview"),
    path("overview/report/", views.overview_report, name="overview_report"),
    path("b/<slug:slug>/", views.brand_dashboard, name="brand_dashboard"),
    path("b/<slug:slug>/report/", views.brand_report, name="brand_report"),
    path("shared/", views.shared_list, name="shared_list"),
    path("shared/new/", views.share_create, name="share_create"),
    path("s/<str:token>/", views.shared_report, name="shared"),
    path("s/<str:token>/refresh/", views.shared_refresh, name="shared_refresh"),
]
