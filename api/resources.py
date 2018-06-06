import json
import logging

from tastypie.resources import Resource
from tastypie.authorization import Authorization
from tastypie.authentication import Authentication, ApiKeyAuthentication
from tastypie.exceptions import ImmediateHttpResponse
from tastypie import fields
from tastypie import http

from .auth import StaffAuthorization
from core.utils import RecordDict
from analytics.collectors.semantic import get_graph
from dataman.normalizer import TweetNormalizer
from dataman.elastic import create_or_update_index, delete_from_index


LOG = logging.getLogger('tweet')


class EdgeBundleResource(Resource):
    name = fields.CharField()
    size = fields.IntegerField()
    children = fields.ListField()
    
    class Meta:
        resource_name = 'edge_bundle'
        allowed_methods = ('get',)
        detail_uri_name = 'name'
        authorization = Authorization()
        authentication = Authentication()

    def dehydrate(self, bundle):
        bundle = super().dehydrate(bundle)
        for fieldname in self.__dict__['fields'].keys():
            try:
                bundle.data[fieldname] = getattr(bundle.obj, fieldname)
            except Exception:
                pass
        return bundle

    def obj_get_list(self, bundle, **kwargs):
        """
        Intercepts obj_get_list to extract fieldname from kwargs
        for aggregation purposes.
        """
        self.term = kwargs.pop(self._meta.detail_uri_name, None)
        objects = []
        for obj in get_graph(self.term):
            objects.append(RecordDict(**obj))
        return self.authorized_read_list(objects, bundle)

    def get_detail(self, request, **kwargs):
        """Simply redirects to obj_get_list."""
        return self.get_list(request, **kwargs)

    def alter_list_data_to_serialize(self, request, data, **kwargs):
        """
        Adds messages to ['meta'].
        """
        data['meta'].update(name=self.term)
        return data


class TweetResource(Resource):
    tweetid = fields.CharField()
    created_at = fields.DateTimeField()
    annotations = fields.DictField()
    geotags = fields.DictField()
    lang = fields.CharField()
    latlong = fields.DictField()
    tweet = fields.DictField()

    class Meta:
        resource_name = 'tweet'
        list_allowed_methods = ['get', 'post']
        detail_allowed_methods = ['get', 'post', 'delete']
        authorization = StaffAuthorization()
        authentication = ApiKeyAuthentication()

    def normalize_object(self, bundle):
        try:
            obj = TweetNormalizer(bundle.data).normalize()
        except Exception as err:
            LOG.error("{}: {}\n---FAILURE REPORT START---\n{}\n---FAILURE REPORT END---\n".format(
                type(err), err, json.dumps(bundle.data, indent=4)))
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))
        else:
            bundle.obj = RecordDict(**obj)
            bundle.obj.pk = obj['tweetid']
        return bundle

    def obj_create(self, bundle, **kwargs):
        self.authorized_create_detail([bundle.data], bundle)
        bundle = self.normalize_object(bundle)
        try:
            result = create_or_update_index(bundle.obj.pk, bundle.obj)
        except Exception as err:
            LOG.error("{}: {}".format(type(err), err))
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))

        # TODO
        # If sucessful `result` is either 'created' or 'updated'
        # This should affect response ('created': 201, 'updated': 200)
        bundle.data.update(result=result)
        return bundle

    def obj_delete(self, bundle, **kwargs):
        self.authorized_delete_detail([bundle.data], bundle)
        try:
            delete_from_index(kwargs['pk'])
        except Exception as err:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))
