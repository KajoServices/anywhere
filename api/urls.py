# -*- coding: utf-8 -*-
"""
URL patterns to API resources.
"""

from django.conf.urls import url, include
from tastypie.api import Api

from .apps import ApiConfig
from . import resources


v1_api = Api(api_name=ApiConfig.name)
v1_api.register(resources.EdgeBundleResource())
v1_api.register(resources.TweetResource())
v1_api.register(resources.CountryResource())
v1_api.register(resources.CategorizedTweetResource())

urlpatterns = [
    url(r'^', include(v1_api.urls)),
]
