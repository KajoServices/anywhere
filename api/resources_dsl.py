# -*- coding: utf-8 -*-

"""
Work in progress:
switching from elasticsearch dict-like queries
to django-style elasticsearch_dsl lib.
"""

from elasticsearch_dsl import Search

from tastypie.resources import ModelResource
from tastypie.authentication import Authentication, ApiKeyAuthentication

from django.conf import settings
from django.utils import timezone

from core.utils import RecordDict
from dataman.elastic import es as es_client
from .auth import UserAuthorization


class DSLDocument(object):
    """
    Container class for the StreamEntryDSLResource.
    """
    _id = 0
    created_at = timezone.now()
    annotations = {}
    geotags = {}
    lang = ''
    latlong = {}
    # _src is a source document with its original structure.
    _src = {}

    def __init__(self):
        methods = [x for x in dir(self) if callable(getattr(self, x))]
        fields = [x for x in self.__dict__.keys()
            if (not x.startswith("__")) and (x not in methods)]
        self._meta = RecordDict(fields=fields)


class DSLQueryset(RecordDict):
    def __init__(self, query, pk_name='_id', **kwargs):
        _meta = RecordDict(
            pk=RecordDict(name=pk_name),
            fields=[]
            )
        kwargs.update(
            model=RecordDict(_meta=_meta),
            query=query
            )
        if 'fields' not in kwargs:
            kwargs.update(fields=[])
        super().__init__(**kwargs)

    def __call__(self):
        return self.query


class StreamEntryDSLResource(ModelResource):
    class Meta:
        resource_name = 'stream'
        list_allowed_methods = ['get', 'post']
        detail_allowed_methods = ['get', 'post', 'delete']
        object_class = DSLDocument
        queryset = DSLQueryset(Search(using=es_client, index=settings.ES_INDEX))
        authorization = UserAuthorization()
        authentication = ApiKeyAuthentication()
