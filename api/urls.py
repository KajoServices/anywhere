# -*- coding: utf-8 -*-
"""
URL patterns to API resources.
"""

from django.conf.urls import url, include
from tastypie.api import Api

from .apps import ApiConfig
from .resources import EdgeBundleResource


v1_api = Api(api_name=ApiConfig.name)
v1_api.register(EdgeBundleResource())

urlpatterns = [
    url(r'^', include(v1_api.urls)),
]
