from django.urls import path
from . import views

urlpatterns = [
    path("", views.api_root),
    path("health/", views.health),
    path("auth/login/", views.auth_login),
    path("auth/register/", views.auth_register),
    path("auth/logout/", views.auth_logout),
    path("credentials/verify/", views.credentials_verify),
    path("chat/", views.chat),
    path("raw/tables/", views.raw_tables),
    path("raw/table/", views.raw_table),
]
