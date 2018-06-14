# -*- coding: utf-8 -*-
from django.conf.urls import url
from django.contrib import admin

from .views import FloodMapView

urlpatterns = [
    url(r'^floodmap/$', FloodMapView.as_view(), name='floodmap'),
    ]
