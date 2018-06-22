# -*- coding: utf-8 -*-
from django.conf.urls import url
from django.contrib import admin

from .views import get_floodmap

urlpatterns = [
    url(r'^floodmap/$', get_floodmap, name='floodmap'),
    ]
