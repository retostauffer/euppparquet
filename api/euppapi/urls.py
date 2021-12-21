"""euppapi URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, re_path

from . import views
from . import api

urlpatterns = [
    path('admin/', admin.site.urls),
    path("", views.home, name = "home sweet home"),
    re_path(r"^test/?$", views.test),
    ## turn that into view/...../???? re_path(r"^api/test/?$", api.show_parquet_data),
    re_path(r"^api/analysis/(?P<daterange>[0-9]{4}-[0-9]{2}-[0-9]{2}(/[0-9]{4}-[0-9]{2}-[0-9]{2})?)/?$", api.get_messages_analysis, name = "API Analysis"),
    re_path(r"^api/forecast/(?P<product>\w+)/(?P<daterange>[0-9]{4}-[0-9]{2}-[0-9]{2}(/[0-9]{4}-[0-9]{2}-[0-9]{2})?)/?$", api.get_messages_forecast, name = "API Forecast"),
    re_path(r"^api/reforecast/(?P<product>\w+)/(?P<daterange>[0-9]{4}-[0-9]{2}-[0-9]{2}(/[0-9]{4}-[0-9]{2}-[0-9]{2})?)/?$", api.get_messages_forecast, name = "API Reforecast"),
]
