from tastypie.resources import Resource
from tastypie.authorization import Authorization
from tastypie.authentication import Authentication
from tastypie.exceptions import ImmediateHttpResponse
from tastypie import fields
from tastypie import http

from core.utils import RecordDict
from analytics.collectors.semantic import get_graph
from dataman.normalizer import TweetNormalizer
from dataman.elastic import create_or_update_index, delete_from_index


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
        authorization = Authorization()
        authentication = Authentication()

    def obj_create(self, bundle, **kwargs):
        try:
            obj = TweetNormalizer(bundle.data).normalize()
            result = create_or_update_index(obj['tweetid'], obj)
        except Exception as err:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))
        # If sucessful `result` is either 'created' or 'updated'
        bundle.data.update(result=result)

        bundle.obj = RecordDict(**obj)
        bundle.obj.pk = obj['tweetid']
        return bundle

    def obj_delete(self, bundle, **kwargs):
        # XXX uncomment when auth is done
        #
        # self.authorized_delete_detail(self.get_object_list(bundle.request), bundle)
        #
        try:
            delete_from_index(kwargs['pk'])
        except Exception as err:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))
