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

from analytics.collectors.semantic import get_graph
from dataman.processors import ClusterBuilder, GeoClusterBuilder, \
     TweetNormalizer, normalize_aggressive, categorize_repr_docs
from dataman.elastic import create_or_update_doc, delete_doc, update_doc, \
     search, FilterConverter, ES_KEYWORDS
from core.utils import RecordDict, flatten_list, avg_coords, \
     MalformedValueError, QUERY_TERMS
from .auth import StaffAuthorization, UserAuthorization


LOG = logging.getLogger('tweet')
MSG_KEYS = ('info', 'warning', 'error',)
DATE_FILTERS = ('exact', 'lt', 'lte', 'gte', 'gt', 'ne')
GEOJSON_HEADER = {
    "type": "FeatureCollection",
    "crs": {
        "type": "name",
        "properties": {
            "name": settings.GEO_CRS
            }
        }
    }


def log_and_raise_400(err):
    LOG.error("{}: {}".format(type(err), err))
    raise ImmediateHttpResponse(response=http.HttpBadRequest(err))


def log_all_ok(result, _id, created_at=None):
    if created_at:
        LOG.info("{}: {} ({})".format(result, _id, created_at))
    else:
        LOG.info("{}: {}".format(result, _id))


def prepare_buckets(key, buckets):
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


class GeoJsonResource(Resource):
    def alter_list_data_to_serialize(self, request, data):
        """
        Re-formats output to meet GeoJSON standard.
        """
        data.update(GEOJSON_HEADER)

        # Rename "objects" to meet GeoJSON format.
        if settings.API_OBJECTS_KEY != "objects":
            data[settings.API_OBJECTS_KEY] = data["objects"]
            del data["objects"]

        return data


# TODO
# * use ES pagination - see
#   https://www.elastic.co/guide/en/elasticsearch/reference/6.1/search-request-from-size.html
# * `tokens` should include synonyms
class TweetResource(GeoJsonResource):
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
            'representative': ('exact',),
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
        authorization = UserAuthorization()
        authentication = ApiKeyAuthentication()

    def __init__(self, *args, **kwargs):
        """
        Adding internal attributesfor filtering, sorting, aggregation, etc.
        """
        super().__init__(*args, **kwargs)
        self.match = {}
        self.filters = {}
        self.sort = {}
        self.aggregate = {}

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
        data = super().alter_list_data_to_serialize(request, data)
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

            buckets = prepare_buckets(key, buckets)
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
        body = {}

        if self.sort:
            body.update({"sort": self.sort})

        # Apply filters (if any).
        if self.filters:
            query = {
                "bool": {
                    "must": self.match,
                    "filter": self.filters
                    }
                }
        else:
            query = self.match
        body.update({"query": query})

        # Adding aggregations.
        if self.aggregate:
            body.update({"aggregations": self.aggregate})

        # Hard limit.
        size = request.GET.get("size", settings.API_LIMIT_PER_PAGE)
        body.update({"size": size})

        queryset = search(body)

        # Collect aggregations and store in the instance-wide variable
        # for injecting in `alter_list_data_to_serialize`
        self.aggregations = self.collect_aggregations(queryset)

        docs = []
        for hit in queryset['hits']['hits']:
            obj = RecordDict(**hit['_source'])
            obj.update({'score': hit['_score'] or 0})
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
        es_filters = {}
        converter = FilterConverter(**filters)
        keywords = ['annotations', 'resource_uri', 'user_id']
        try:
            converted_filters = converter.convert(
                schema=self._meta.filtering, keywords=keywords
                )
        except MalformedValueError as err:
            raise ImmediateHttpResponse(response=http.HttpBadRequest(err))

        if len(converted_filters) > 1:
            es_filters.update({"bool": {"must": converted_filters}})
        elif converted_filters:
            es_filters.update(converted_filters[0])

        return es_filters

    def get_order_by(self, **kwargs):
        size = int(kwargs.get('size', settings.ES_MAX_RESULTS))
        if ("search" in kwargs) or (size == 0):
            return []

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
        order_keys = flatten_list([x.keys() for x in order_by])
        if settings.ES_TIMESTAMP_FIELD not in order_keys:
            order_by.append({
                settings.ES_TIMESTAMP_FIELD: {"order": "desc"}
                })
        return order_by

    def get_aggregate_by(self, **filters):
        aggregate_by = {}
        if "agg_timestamp" in filters:
            interval = filters.get("agg_timestamp__interval", settings.TIMESTAMP_INTERVAL)
            aggregate_by.update({
                "agg_timestamp": {
                    "date_histogram": {
                        "field": settings.ES_TIMESTAMP_FIELD,
                        "interval": interval
                        }
                    }
                })
        if "agg_floodprob" in filters:
            interval = filters.get("agg_floodprob__interval", settings.TIMESTAMP_INTERVAL)
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
        if "agg_hotspot" in filters:
            precision = filters.get("agg_hotspot__precision", settings.HOTSPOTS_PRECISION)
            size = filters.get("agg_hotspot__size", settings.HOTSPOTS_MAX_NUMBER)
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
        if hasattr(bundle.request, "GET"):
            filters = bundle.request.GET.dict()
        filters.update(kwargs)

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
        resource_name = "tweet_categorized"
        list_allowed_methods = ("get", "delete")
        detail_uri_name = "tweetid"
        detail_allowed_methods = []
        filtering = {
            "flood_probability": ("gte",),
            "lang": ("exact",),
            "country": ("exact",),
            }
        ordering = [
            settings.ES_TIMESTAMP_FIELD,
            "flood_probability",
            "country",
            "lang",
            "user_lang",
            "user_name",
            "user_time_zone",
            ]
        match_fields = [
            "text", "tokens", "place", "user_name",
            "user_location", "user_description",
            ]
        authorization = StaffAuthorization()
        authentication = ApiKeyAuthentication()

    def _prepare_categorized(self, container):
        for key in container.keys():
            container[key] = [
                RecordDict(**{
                    "_id": doc["_id"],
                    "text": doc["text"],
                    "_multiplicity": doc["_multiplicity"],
                    "_centrality": doc["_centrality"]
                    })
                for doc in container[key]
                ]
        return container

    def _categorize_list(self, objects):
        """
        Categorizes list of objects to 'representative' and non-representative'.
        """
        docs = []
        for obj in objects:
            # This can either be bundle or object.
            try:
                doc = obj.obj.copy()
            except AttributeError:
                doc = obj.copy()
            doc.update({
                "_id": doc[self._meta.detail_uri_name],
                "_normalized_text": normalize_aggressive(doc["text"])
                })
            docs.append(doc)
        categorized = categorize_repr_docs(docs)
        prepared = self._prepare_categorized(categorized)
        return [{"docs": prepared}]

    def _categorize_clusters(self, request, terms):
        """
        Categorizes documents to 'representative' and non-representative',
        seperating them to segments given by terms (procided by user).

        Does not require list of objects, operates on user-provided filters.
        """
        filters = request.GET.dict()
        if settings.ES_GEO_FIELD in terms:
            # Clustering tweets by geolocation is different.
            terms = tuple(x for x in terms if x != settings.ES_GEO_FIELD)
            cb = GeoClusterBuilder(*terms, **filters)
        else:
            cb = ClusterBuilder(*terms, **filters)
        clusters = cb.get_clusters()

        # Select representative tweets for each cluster.
        for cluster in clusters.clusters:
            categorized = categorize_repr_docs(cluster["docs"])
            cluster["docs"] = self._prepare_categorized(categorized)
        return clusters.clusters

    def _categorize(self, request, objects):
        terms = request.GET.get('terms', '')
        if isinstance(terms, str):
            terms = [x.strip() for x in terms.split(',') if x.strip() != '']
        assert type(terms) in [list, tuple], \
            "Wrong terms type! Must be list or tuple!"

        if terms:
            categorized = self._categorize_clusters(request, terms)
        else:
            categorized = self._categorize_list(objects)
        return categorized

    def alter_list_data_to_serialize(self, request, data):
        if request.method != 'GET':
            return data

        categorized = self._categorize(request, data['objects'])
        data.update(categorized=categorized)
        return data

    def _delete_docs(self, objects_list, categorized):
        """
        Actual deletion of non-representative tweets.
        """
        ids_to_delete = []
        for cluster in categorized:
            for doc in cluster["docs"]["non_representative_docs"]:
                ids_to_delete.append(doc["_id"])

        objects_to_delete = [x for x in objects_list if x["tweetid"] in ids_to_delete]
        deletable_objects = self.authorized_delete_list(objects_to_delete, bundle)
        if hasattr(deletable_objects, 'delete'):
            # It's likely a ``QuerySet``. Call ``.delete()`` for efficiency.
            deletable_objects.delete()
        else:
            for authed_obj in deletable_objects:
                try:
                    result = delete_doc(authed_obj["tweetid"])
                except Exception as err:
                    log_and_raise_400(err)
                else:
                    log_all_ok(result, authed_obj["tweetid"])

    # TODO
    # re-assign this to PATCH! DELETE should delete!
    def obj_delete_list(self, bundle, **kwargs):
        """
        delete_list doesn't actually delete anything - except it marks
        analyzes tweets and marks them as representative or non-representative.
        """
        objects_list = self.obj_get_list(bundle=bundle, **kwargs)
        categorized = self._categorize(bundle.request, objects_list)

        # Mark categorized docs
        for cluster in categorized:
            for doc in cluster["docs"]["non_representative_docs"]:
                update_doc(doc["_id"], representative=False)
            for doc in cluster["docs"]["representative_docs"]:
                update_doc(doc["_id"], representative=True)

        # XXX actual deletion
        # self._delete_docs(objects_list, categorized)


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
        data["objects"] = [r['NAME'] for r in DBF(settings.WORLD_BORDERS)]
        cnt = len(data["objects"])
        data["meta"].update({"total_count": cnt, "limit": cnt})
        return data

    def obj_get_list(self, bundle, **kwargs):
        """Dummy."""
        return []
