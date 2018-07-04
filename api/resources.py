import json
import logging
from dbfread import DBF

from django.conf import settings
from django.db.models.constants import LOOKUP_SEP

from tastypie.resources import Resource
from tastypie.authorization import Authorization
from tastypie.authentication import Authentication, ApiKeyAuthentication
from tastypie.exceptions import ImmediateHttpResponse, InvalidFilterError, \
     InvalidSortError
from tastypie import fields
from tastypie import http

from .auth import StaffAuthorization
from analytics.collectors.semantic import get_graph
from dataman.processors import TweetNormalizer, normalize_aggressive, \
     categorize_repr_docs
from dataman.elastic import search, create_or_update_doc, delete_doc, \
     FilterConverter, ES_KEYWORDS
from core.utils import RecordDict, flatten_list, avg_coords, QUERY_TERMS


LOG = logging.getLogger('tweet')
MSG_KEYS = ('info', 'warning', 'error',)
DATE_FILTERS = ('exact', 'lt', 'lte', 'gte', 'gt', 'ne')


def log_and_raise_400(err):
    LOG.error("{}: {}".format(type(err), err))
    raise ImmediateHttpResponse(response=http.HttpBadRequest(err))


def log_all_ok(result, _id, created_at=None):
    if created_at:
        LOG.info("{}: {} ({})".format(result, _id, created_at))
    else:
        LOG.info("{}: {}".format(result, _id))


def prepare_buckets(key, buckets, **filters):
    """
    Re-formats buckets for cleaner look.
    """
    if key == 'agg_hotspot':
        buckets = [b for b in buckets
                   if int(b['doc_count']) >= settings.HOTSPOT_MIN_ENTRIES]
        buckets = buckets[:settings.HOTSPOTS_MAX_NUMBER]
        _comp = lambda x: dict((b['key'], b['doc_count']) for b in x['buckets'])
        for buck in buckets:
            buck['location'] = avg_coords(buck['cell']['bounds'])
            del buck['cell']
    elif key == 'agg_floodprob':
        for bucket in buckets:
            bucket['avg_flood_probability'] = bucket['avg_flood_probability']['value']
    return buckets


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


# TODO
# * self.fields is optimised for POST now. Re-factor it for GET
#   (less headache with filtering and hydrate)
# * use ES pagination - see
#   https://www.elastic.co/guide/en/elasticsearch/reference/6.1/search-request-from-size.html
# * `tokens` should include synonyms
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
        detail_uri_name = 'tweetid'
        filtering = {
            'flood_probability': ('gte',),
            'lang': ('exact',),
            'country': ('exact',),
            }
        ordering = [
            settings.ES_TIMESTAMP_FIELD,
            'flood_probability',
            'country',
            'lang',
            'user_lang',
            'user_name',
            'user_time_zone',
            ]
        match_fields = [
            "text", "tokens", "place", "user_name",
            "user_location", "user_description",
            ]
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
        return bundle

    def obj_create(self, bundle, **kwargs):
        self.authorized_create_detail([bundle.data], bundle)
        bundle = self.normalize_object(bundle)
        _id = getattr(bundle.obj, self._meta.detail_uri_name)
        try:
            result = create_or_update_doc(_id, bundle.obj)
        except Exception as err:
            log_and_raise_400(err)
        else:
            log_all_ok(result, _id, bundle.obj.created_at)

        # TODO
        # If sucessful `result` is either 'created' or 'updated'
        # This should affect response ('created': 201, 'updated': 200)
        bundle.data.update(result=result)
        return bundle

    def obj_delete(self, bundle, **kwargs):
        self.authorized_delete_detail([bundle.data], bundle)
        _id = kwargs[self._meta.detail_uri_name]
        try:
            result = delete_doc(_id)
        except Exception as err:
            log_and_raise_400(err)
        else:
            log_all_ok(result, _id)

    def check_filtering(self, field_name, filter_type='exact', filter_bits=None):
        if filter_bits is None:
            filter_bits = []
        if field_name not in self._meta.filtering:
            raise InvalidFilterError("The '%s' field does not allow filtering." % field_name)

        # Check to see if it's an allowed lookup type.
        if self._meta.filtering[field_name] not in QUERY_TERMS:
            # Must be an explicit whitelist.
            if filter_type not in self._meta.filtering[field_name]:
                raise InvalidFilterError(
                    "'%s' is not an allowed filter on the '%s' field." % (
                        filter_type, field_name
                        ))

        if self.fields[field_name].attribute is None:
            raise InvalidFilterError(
                "The '%s' field has no 'attribute' for searching with." % field_name
                )
        return [self.fields[field_name].attribute]

    def dehydrate(self, bundle):
        """
        Formats output (bundle.data) to meet GeoJSON.
        NB: GeoJSON requires [lon, lat].
        """
        bundle = super().dehydrate(bundle)
        if bundle.request.method == 'GET':
            properties = bundle.obj.copy()
            properties.update({"id": bundle.obj.tweetid})

            # TODO: clearing fields should be done automatically by
            #       specifying resource fields!
            #
            # Legacy, surrogate and unnecessary fields.
            exclude_fields = [
                "location", "latlong", "geotags", "annotations",
                "tweet", "tweetid"
                ]
            for field in exclude_fields:
                try:
                    del properties[field]
                except KeyError:
                    continue

            bundle.data = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        bundle.obj.location["lon"],
                        bundle.obj.location["lat"]
                        ]
                    },
                "properties": properties
                }
        return bundle

    def alter_list_data_to_serialize(self, request, data):
        """
        Re-formats output to meet GeoJSON standard.
        """
        # Add header required by GeoJSON.
        data.update({
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {
                    "name": "EPSG:4326"
                    }
                }
            })

        # Rename "objects" to meet GeoJSON format.
        data["features"] = data["objects"]
        del data["objects"]

        # Append aggregations if any.
        if self.aggregations:
            data.update({"aggregations": self.aggregations})

        return data

    def collect_aggregations(self, queryset):
        aggregations = {}
        for key in self.aggregate.keys():
            try:
                buckets = queryset['aggregations'][key]['buckets']
            except Exception as err:
                raise ImmediateHttpResponse(response=http.HttpBadRequest(err))

            buckets = prepare_buckets(key, buckets, **self.filters)
            aggregations.update({key: buckets})

        return aggregations

    def apply_filters(self, request):
        """
        :param:match - dict, format:
            {"match_all": {}} (if no search is performed)
            OR
            {"multi_match": {"query": query}} (if there is a query)

        :param:filters - dict, format:
            {"bool": {"must" : [{filter_1}, {filter_2}, ...]}}

        :param:sort - list, format:
            [{"fieldname": {"order": "asc|desc"}}, {...}]
        """
        body = {
            "query": {
                "bool" : {
                    "must": self.match,
                    "filter": self.filters
                    }
                },
            "sort": self.sort,
            "size": settings.ES_MAX_RESULTS
            }
        if self.aggregate:
            body.update({"aggregations": self.aggregate})

        queryset = search(body)

        # Collect aggregations and store in the instance-wide variable
        # for injecting in `alter_list_data_to_serialize`
        self.aggregations = self.collect_aggregations(queryset)

        docs = []
        for hit in queryset['hits']['hits']:
            obj = RecordDict(**hit['_source'])
            obj.update({'score': hit['_score']})
            docs.append(obj)
        return docs

    def build_query(self, **filters):
        query = filters.get("search", None)
        if query is None:
            return {"match_all": {}}

        return {
            "multi_match": {
                "query": query,
                "fields": self._meta.match_fields
                }
            }

    def build_filters(self, **filters):
        converter = FilterConverter(**filters)
        keywords = ['annotations', 'resource_uri', 'user_id']
        es_filters = converter.convert(
            schema=self._meta.filtering, keywords=keywords
            )
        return {"bool": {"must": es_filters}}

    def get_order_by(self, **kwargs):
        order_by_request = kwargs.get('order_by', [])
        if isinstance(order_by_request, str):
            order_by_request = [order_by_request]

        order_by = []
        for fieldname in order_by_request:
            if not isinstance(fieldname, str):
                continue

            order_by_bits = fieldname.split(LOOKUP_SEP)
            name = order_by_bits[0]
            order = {"order": "asc"}
            if order_by_bits[0].startswith('-'):
                name = order_by_bits[0][1:]
                order = {"order": "desc"}

            # XXX un-comment this after re-factoring in favor for GET object structure
            #
            # if name not in self.fields:
            #     # It's not a field we know about.
            #     raise InvalidSortError("No matching '%s' field for ordering on." % field_name)
            if name not in self._meta.ordering:
                raise InvalidSortError(
                    "The '%s' field does not allow ordering." % name
                    )

            # Add .keyword to string fields.
            if name in ES_KEYWORDS:
                name = '{}.keyword'.format(name)
            order_by.append({name: order})

        # `search` param defines sorting order:
        # - if search term is present, sort only by _score (relevant
        #   tweets at the top)
        # - otherwise add timestamp to the end of list (will be default
        #   if sort isn't specified)
        if kwargs.get('search', False):
            order_by = [{"_score": {"order": "desc"}}]
        else:
            order_keys = flatten_list([x.keys() for x in order_by])
            if settings.ES_TIMESTAMP_FIELD not in order_keys:
                order_by.append({
                    settings.ES_TIMESTAMP_FIELD: {"order": "desc"}
                    })
        return order_by

    def get_aggregate_by(self, **filters):
        aggregate_by = {}
        if filters.get('agg_timestamp', False):
            interval = filters.get("agg_precision", settings.TIMESTAMP_PRECISION)
            aggregate_by.update({
                "agg_timestamp": {
                    "date_histogram": {
                        "field": settings.ES_TIMESTAMP_FIELD,
                        "interval": interval
                        }
                    }
                })
        if filters.get('agg_floodprob', False):
            interval = filters.get("agg_precision", settings.TIMESTAMP_PRECISION)
            aggregate_by.update({
                "agg_floodprob": {
                    "date_histogram": {
                        "field": settings.ES_TIMESTAMP_FIELD,
                        "interval": interval
                        },
                    "aggs": {
                        "avg_flood_probability": {
                            "avg": {
                                "field": "flood_probability"
                                }
                            }
                        }
                    }
                })
        if filters.get('agg_hotspot', False):
            precision = filters.get("agg_precision", settings.HOTSPOTS_PRECISION)
            size = filters.get("agg_size", settings.HOTSPOTS_MAX_NUMBER)
            aggregate_by.update({
                "agg_hotspot": {
                    "geohash_grid": {
                        "field": "location",
                        "precision": precision,
                        "size": size,
                        },
                    "aggs": {
                        "cell": {
                            "geo_bounds": {
                                "field": "location"
                                }
                            }
                        }
                    }
                })
        return aggregate_by

    def obj_get_list(self, bundle, **kwargs):
        filters = {}
        self.messages = dict((x, []) for x in MSG_KEYS)
        if hasattr(bundle.request, 'GET'):
            filters = bundle.request.GET.dict()
        filters.update(kwargs)

        # XXX __init__ these all
        self.match = self.build_query(**filters)
        self.filters = self.build_filters(**filters)
        self.sort = self.get_order_by(**filters)
        self.aggregate = self.get_aggregate_by(**filters)
        try:
            objects = self.apply_filters(bundle.request)
        except ValueError:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(
                "Invalid resource lookup data provided (mismatched type)."
                ))

        return self.authorized_read_list(objects, bundle)


class CategorizedTweetResource(TweetResource):
    class Meta:
        resource_name = 'tweet_categorized'
        list_allowed_methods = ('get',)
        detail_uri_name = 'tweetid'
        detail_allowed_methods = []
        filtering = {
            'flood_probability': ('gte',),
            'lang': ('exact',),
            'country': ('exact',),
            }
        ordering = [
            settings.ES_TIMESTAMP_FIELD,
            'flood_probability',
            'country',
            'lang',
            'user_lang',
            'user_name',
            'user_time_zone',
            ]
        match_fields = [
            "text", "tokens", "place", "user_name",
            "user_location", "user_description",
            ]
        authorization = StaffAuthorization()
        authentication = ApiKeyAuthentication()

    def alter_list_data_to_serialize(self, request, data):
        def reduce_categorized(doc):
            doc_reduced = dict((k, doc[k]) for k in doc.keys() if k in ("_id", "text"))
            return RecordDict(**doc_reduced)
            
        docs = []
        for obj in data['objects']:
            doc = obj.obj.copy()
            doc.update({
                "_id": getattr(obj.obj, self._meta.detail_uri_name),
                "_normalized_text": normalize_aggressive(obj.obj.text)
                })
            docs.append(doc)
        
        categorized = categorize_repr_docs(docs)
        representative_docs = []
        for doc in categorized["representative_docs"]:
            representative_docs.append(reduce_categorized(doc))

        non_representative_docs = []
        for doc in categorized["non_representative_docs"]:
            non_representative_docs.append(reduce_categorized(doc))

        data.update(
            representative_docs=representative_docs,
            non_representative_docs=non_representative_docs
            )
        return data


class CountryResource(Resource):
    """
    Plain and simple list of countries.
    """
    class Meta:
        resource_name = 'country'
        list_allowed_methods = ('get',)
        detail_allowed_methods = []
        authorization = Authorization()
        authentication = Authentication()

    def alter_list_data_to_serialize(self, request, data):
        """Serves plain list of country names."""
        data["objects"] = [r['NAME'] for r in DBF(settings.COUNTRIES)]
        cnt = len(data["objects"])
        data["meta"].update({"total_count": cnt, "limit": cnt})
        return data

    def obj_get_list(self, bundle, **kwargs):
        """Dummy."""
        return []
