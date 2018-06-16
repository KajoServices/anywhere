import json
import logging

from django.conf import settings
from django.utils import timezone
from django.db.models.constants import LOOKUP_SEP

from tastypie.resources import Resource
from tastypie.authorization import Authorization
from tastypie.authentication import Authentication, ApiKeyAuthentication
from tastypie.exceptions import ImmediateHttpResponse, InvalidFilterError
from tastypie.utils import dict_strip_unicode_keys, string_to_python
from tastypie import fields
from tastypie import http

from .auth import StaffAuthorization
from analytics.collectors.semantic import get_graph
from dataman.normalizer import TweetNormalizer
from dataman.elastic import search, create_or_update_index, delete_from_index
from core.utils import RecordDict, get_parsed_datetime, \
     localize_timestamp, convert_time_range, flatten_list


LOG = logging.getLogger('tweet')
MSG_KEYS = ('info', 'warning', 'error',)
QUERY_TERMS = [
    'ne', 'lt', 'lte', 'gt', 'gte', 'not', 'in', 'nin', 'mod', 'all', 'size',
    'exists', 'exact', 'iexact', 'contains', 'icontains', 'startswith',
    'istartswith', 'endswith', 'iendswith', 'match'
    ]
DATE_FILTERS = ('exact', 'lt', 'lte', 'gte', 'gt', 'ne')
TS_GTE = settings.ES_TIMESTAMP_FIELD + '__gte'
TS_LT = settings.ES_TIMESTAMP_FIELD + '__lte'
GEO_FIELDS = [
    'top_left_lon', 'top_left_lat', 'bottom_right_lon', 'bottom_right_lat'
    ]


def avg_coords(rec):
    _lng, _lat = 0, 0
    count = len(rec)
    for each in rec:
        _lng += rec[each]['lon']
        _lat += rec[each]['lat']
    return [_lng*1.0/count, _lat*1.0/count]


def build_filters_geo(filters):
    if not all([k in filters for k in GEO_FIELDS]):
        return {}

    return {
        "geo_bounding_box": {
            "location": {
                "top_left" : {
                    "lat": float(filters["top_left_lat"]),
                    "lon": float(filters["top_left_lon"])
                    },
                "bottom_right" : {
                    "lat": float(filters["bottom_right_lat"]),
                    "lon": float(filters["bottom_right_lon"])
                    }
                }
            }
        }

def build_filters_time(filters):
    """
    Re-defines filters dict, taiking into account custom timestamp
    filters: all keys in `filters` that contain 'timestamp' are
    being converted into the form
        timestamp[__<modifier>]=<datetime>

    Other filters remain intact.

    :param filters: dict
    :param limit_ctrl: bool - if True, lower limit will be set
        if there is no 'timestamp' in `filters`.

    :return: dict - modified filters.
    """
    def filter_exact(val):
        ts, _ = localize_timestamp(val)
        return {
            TS_GTE: ts,
            TS_LT: ts + timezone.timedelta(minutes=1)
            }

    result = {}
    for key, val in filters.items():
        if not settings.ES_TIMESTAMP_FIELD in key:
            continue

        if '__' in key:
            # "{ES_TIMESTAMP_FIELD}__exact" is not allowed! Use
            # it as a lower limit, and get a value one minute
            # later for a higher limit.
            if '__exact' in key:
                result.update(filter_exact(val))
            else:
                ts = get_parsed_datetime(val)
                ts, _ = localize_timestamp(ts)
                result[key] = ts
            continue

        # Process '{ES_TIMESTAMP_FIELD}='.
        try:
            value = get_parsed_datetime(val)
        except TypeError:
            ts_from, ts_to = convert_time_range(val)
            result.update({TS_GTE: ts_from, TS_LT: ts_to})
        else:
            # The same as {ES_TIMESTAMP_FIELD}__exact.
            result.update(filter_exact(val))

    timestamp_range = {}
    if TS_GTE in filters:
        timestamp_range.update({"gte": result[TS_GTE].isoformat()})
    if TS_LT in filters:
        timestamp_range.update({"lt": result[TS_LT].isoformat()})
    if timestamp_range:
        timestamp_range = {"range": {"created_at": timestamp_range}}

    return timestamp_range


def get_hotspots(ids):
    body = {
        "query": {
            "terms": {
                "tweetid": ids
                }
            },
        "aggregations": {
            "geo_hotspots": {
                "geohash_grid": {
                    "field": "location",
                    "precision": settings.HOTSPOTS_PRECISION,
                    "size": settings.HOTSPOTS_MAX_NUMBER,
                    },
                "aggs": {
                    "cell": {
                        "geo_bounds": {
                            "field": "location"
                            }
                        }
                    }
                }
            }
        }
    res = search(body)
    try:
        buckets = res['aggregations']['geo_hotspots']['buckets']
    except Exception as err:
        return 'ERROR retrieving hotspots. %s: %s' % (type(err), str(err))

    buckets = [b for b in buckets
               if int(b['doc_count']) >= settings.HOTSPOT_MIN_ENTRIES]
    buckets = sorted(buckets, key=lambda x: x['doc_count'], reverse=True)
    buckets = buckets[:settings.HOTSPOTS_MAX_NUMBER]
    _comp = lambda x: dict((b['key'], b['doc_count']) for b in x['buckets'])
    for buck in buckets:
        buck['location'] = avg_coords(buck['cell']['bounds'])
        del buck['cell']
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
        ordering = [settings.ES_TIMESTAMP_FIELD]
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
            result = create_or_update_index(_id, bundle.obj)
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
            delete_from_index(kwargs[self._meta.detail_uri_name])
        except Exception as err:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))

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
        bundle = super().dehydrate(bundle)
        if bundle.request.method == 'GET':
            bundle.data.update(bundle.obj)
        return bundle

    def alter_list_data_to_serialize(self, request, data):
        """
        Add hotspots to the list of returned results after pagination.
        """
        hotspots = get_hotspots([x.obj.tweetid for x in data['objects']])
        data.update(hotspots=hotspots)
        return data

    def apply_filters(self, request, match=None, filters=None, sort=None):
        """
        :param:applicable_filters - dict in the form
            {"bool": {"must" : [{filter_1}, {filter_2}, ...]}}
        """
        if filters is None:
            filters = {}

        if sort is None:
            sort = []

        # `match` defines sorting order:
        # - if there is search, sort only by _score (relevant tweets at the top)
        # - otherwise add timestamp to the end of list (will be default if sort
        #   isn't specified)
        if match is None:
            match = {"match_all": {}}
            sort_keys = flatten_list([x.keys() for x in sort])
            if settings.ES_TIMESTAMP_FIELD not in sort_keys:
                sort.append({
                    settings.ES_TIMESTAMP_FIELD: {"order" : "desc"}
                    })
        else:
            sort = [{"_score": {"order" : "desc"}}]

        body = {
            "query": {
                "bool" : {
                    "must": match,
                    "filter": filters
                    }
                },
            "sort": sort,
            'size': settings.ES_MAX_RESULTS
            }

        result = []
        for hit in search(body)['hits']['hits']:
            obj = RecordDict(**hit['_source'])
            obj.update({'score': hit['_score']})
            result.append(obj)

        return result

    def build_query(self, filters):
        query = filters.get("search", None)
        if query is None:
            return {"match_all": {}}

        return {
            "multi_match": {
                "query": query,
                "fields": [
                    "text", "tokens", "place", "user_name",
                    "user_location", "user_description",
                    ]
                }
            }

    def build_filters(self, filters=None, ignore_bad_filters=True):
        if filters is None:
            return {}

        es_filters = []

        filters_geo = build_filters_geo(filters)
        if filters_geo:
            es_filters.append(filters_geo)

        filters_time = build_filters_time(filters)
        if filters_time:
            es_filters.append(filters_time)

        for filter_expr, value in filters.items():
            filter_bits = filter_expr.split(LOOKUP_SEP)
            field_name = filter_bits.pop(0)

            # Ignore fields we know nothing about.
            if field_name not in self._meta.filtering:
                continue

            if len(filter_bits) and filter_bits[-1] in QUERY_TERMS:
                filter_type = filter_bits.pop()
                es_filters.append({
                    "range": {
                        field_name: {
                            filter_type: value
                            }
                        }
                    })
            else:
                es_filters.append({
                    "term": {
                        field_name: value
                        }
                    })

        return {"bool": {"must": es_filters}}

    def get_sort_order(self, filters):
        return [] # XXX finish it!

    def obj_get_list(self, bundle, **kwargs):
        filters = {}
        self.messages = dict((x, []) for x in MSG_KEYS)
        if hasattr(bundle.request, 'GET'):
            filters = bundle.request.GET.dict()
        filters.update(kwargs)

        match_query = self.build_query(filters=filters)
        applicable_filters = self.build_filters(filters=filters)
        sort_order = self.get_sort_order(filters=filters)
        try:
            objects = self.apply_filters(
                bundle.request, match_query, applicable_filters, sort_order
                )
        except ValueError:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(
                "Invalid resource lookup data provided (mismatched type)."
                ))
        return self.authorized_read_list(objects, bundle)
