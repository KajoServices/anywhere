import re
import json
import copy
import logging
import dpath.util
from collections import MutableMapping
from decimal import Decimal
from Levenshtein import ratio
from polyglot.text import Text

from django.conf import settings
from django.utils import timezone

from dataman.elastic import search, tokenize, FilterConverter, QueryConverter, \
     ES_INDEX_MAPPING, ES_KEYWORDS
from countries import countries
from core.utils import RecordDict, get_val_by_path, flatten_dict, \
     get_place_coords, avg_coords_list, meters, get_parsed_datetime, \
     build_filters_geo, build_filters_time, \
     UnsupportedValueError, MissingDataError


cc = countries.CountryChecker(settings.WORLD_BORDERS)

LOG = logging.getLogger("tweet")
SIMILAR_PREDICTION_EDIT_DISTANCE_MAX = 0.8


def normalize_aggressive(text):
    """
    Performs aggressive normalization of text
    """
    # Ampersand
    text = re.sub(r'\s+&amp;?\s+', ' and ', text)
    # User mentions
    text = re.sub(r'@[A-Za-z0-9_]+\b', '_USER_ ', text)
    # Time
    text = re.sub(r"\b\d\d?:\d\d\s*[ap]\.?m\.?\b", '_TIME_', text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d\d?\s*[ap]\.?m\.?\b", '_TIME_', text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d\d?:\d\d:\d\d\b", '_TIME_', text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d\d?:\d\d\b", '_TIME_', text, flags=re.IGNORECASE)
    # URLs
    text = re.sub(r'\bhttps?:\S+', ' _URL_ ', text, flags=re.IGNORECASE)
    # Broken URL at the end of a line
    text = re.sub(r'\s+https?$', ' _URL_', text, flags=re.IGNORECASE)
    # Non-alpha non-punctuation non-digit characters
    text = re.sub(r'[^\w\d\s:\'",.\(\)#@\?!/’_]+', '', text)
    # Newlines and double spaces
    text = re.sub(r'\n', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    # Strip
    text = text.strip()
    return text


def categorize_repr_docs(docs):
    def incr(container, key, inc_by=1):
        try:
            container[key] += inc_by
        except KeyError:
            container[key] = inc_by

    is_duplicate = {}

    # Remove docs that have similar probability to another and very similar text
    # This exploits the fact that similar docs will have similar probabilities
    multiplicity = {}
    for tu in docs:
        for tv in docs:
            similarity = ratio(tu["_normalized_text"], tv["_normalized_text"])
            if similarity > SIMILAR_PREDICTION_EDIT_DISTANCE_MAX:
                # The newer doc (larger id) is marked as a duplicate of the older (smaller id) doc
                # Count the == in case there are duplicate ids in the
                if int(tu["_id"]) < int(tv["_id"]):
                    incr(multiplicity, tu["_id"])

    # Add multiplicity for doc in docs:
    for doc in docs:
        if doc["_id"] in multiplicity:
            doc["_multiplicity"] = multiplicity[doc["_id"]]
        else:
            doc["_multiplicity"] = 1

    # Create set for second pass (centrality)
    centrality = {}
    for tu in docs:
        for tv in docs:
            similarity = ratio(tu["_normalized_text"], tv["_normalized_text"])
            # Compute centrality as sum of similarities
            incr(centrality, tu["_id"], similarity)
            incr(centrality, tv["_id"], similarity)

            # Discard duplicates
            if similarity > SIMILAR_PREDICTION_EDIT_DISTANCE_MAX:
                if int(tu["_id"]) < int(tv["_id"]):
                    is_duplicate[tv["_id"]] = tu["_id"]

    # Add centrality and mark centrality=0.0 for duplicates
    for doc in docs:
        if doc["_id"] in centrality and not doc["_id"] in is_duplicate:
            doc["_centrality"] = centrality[doc["_id"]]
        else:
            doc["_centrality"] = 0.

    # Sort by multiplicity and probability of being relevant
    docs_sorted = sorted(
        docs,
        key=lambda x: int(int(x['_multiplicity'])*int(x['_centrality'])),
        reverse=True
        )

    # Separate representative and non-representative docs.
    repr_docs = []
    non_repr_docs = []
    for doc in docs_sorted:
        if doc["_centrality"] > 0.:
            repr_docs.append(doc)
        else:
            non_repr_docs.append(doc)

    return {
        "representative_docs": repr_docs,
        "non_representative_docs": non_repr_docs
        }


def extract_hatshtags(val):
    tags = []
    if isinstance(val, str):
        tags = [x.strip("#.,-\"\'&*^!") for x in val.split()
                if (x.startswith("#") and len(x) < 256)]
    elif isinstance(val, list):
        for entity in val:
            if not isinstance(entity, dict):
                continue

            if "hashtags" in entity:
                try:
                    tags.extend(entity["hashtags"])
                except:
                    pass
            else:
                try:
                    tags = [x["text"].strip() for x in val]
                except (KeyError, AttributeError, TypeError):
                    pass
    return tags


def collect_hashtags(data, hashtags):
    """
    Recursively collects hashtags from the all keys of a tweet.
    """
    for val in data.values():
        if isinstance(val, MutableMapping):
            _hatshtags = collect_hashtags(val, hashtags)
        else:
            _hatshtags = extract_hatshtags(val)

        hashtags.extend(_hatshtags)
    return list(set(hashtags))


def collect_media_urls(data, media_urls):
    """
    Recursively collects media urls from all keys of a tweet.
    """
    for val in data.values():
        if isinstance(val, MutableMapping):
            _media_urls = collect_media_urls(val, media_urls)
        elif isinstance(val, list):
            _media_urls = []
            for item in val:
                try:
                    _media_urls.append(item["media_url"])
                except (KeyError, TypeError):
                    pass
                try:
                    _media_urls.append(item["media_url_https"])
                except (KeyError, TypeError):
                    pass
        else:
            continue

        media_urls.extend(_media_urls)
    return list(set(media_urls))


class TweetNormalizer(object):
    preserve_paths = [
        "id", "tweetid", "text", "lang", "created_at", "ttype", "annotations",
        "geotags", "latlong", "annotations_combined_model", "mordecai_raw",
        "geo", "coordinates", "place", "media_urls",
        "user/id", "user/name", "user/screen_name", "user/location",
        "user/description", "user/followers_count", "user/friends_count",
        "user/listed_count", "user/favourites_count", "user/statuses_count",
        "user/created_at", "user/utc_offset", "user/time_zone",
        "user/lang", "user/profile_image_url",
        ]
    exclude_from_flatten = ["location"]

    def __init__(self, doc, **kwargs):
        self.original = doc
        assert isinstance(self.original, (dict, str)), \
            "Wrong type: must be string or dict"
        if isinstance(self.original, str):
            self.original = json.loads(self.original)

        try:
            self.original["annotations"]
        except KeyError:
            raise MissingDataError(
                "Record {} does not contain 'annotations'!".format(self.original["id_str"]))

        if "tweet" in self.original.keys():
            # Restrcture original doc: place everything at the same level.
            doc_tweet = copy.deepcopy(self.original["tweet"])
            del self.original["tweet"]
            self.original.update(doc_tweet)

        self.normalized = copy.deepcopy(self.original)

    def restructure(self, **kwargs):
        """
        Leaves in place only elements by given paths.

        :param preserve_paths: list of str - fields to preserve
            (field names and paths).
        :return: dict.
        """
        paths = kwargs.get("preserve_paths", [])
        paths.extend(self.preserve_paths)
        paths = list(set(paths))

        subtrees = {}
        for path in paths:
            val = get_val_by_path(path, **self.normalized)
            if not "/" in path:
                self.normalized[path] = val
            else:
                dpath.util.new(subtrees, path, val)

        self.normalized.update(subtrees)

    def fill_annotations(self):
        try:
            annot = self.original["annotations"]
        except KeyError:
            raise Exception("Record must contain 'annotations'!")

        try:
            flood_probability = annot["flood_probability"]
        except (KeyError, AttributeError):
            raise Exception("Record annotations must contain `flood_probability`!")
        assert isinstance(flood_probability, (Decimal, float, list)), "Wrong type: must be float or list!"
        if isinstance(flood_probability, list):
            flood_probability = flood_probability[1] if flood_probability[0] == "yes" else 0

        try:
            location = {
                "lat": self.original["latlong"]["lat"],
                "lon": self.original["latlong"]["long"]
                }
        except TypeError:
            location = {
                "lat": self.original["latlong"][0],
                "lon": self.original["latlong"][1]
                }
        except KeyError:
            raise Exception("Record must contain 'latlong'!")

        try:
            geotags = self.original["geotags"]
        except:
            country = None
            place = None
        else:
            country = geotags.get("country_predicted", None)
            place = geotags.get("place_name", None)

        self.normalized.update({
            "tweetid": self.original["tweetid"],
            "created_at": self.original["created_at"],
            "lang": self.original["lang"],
            "flood_probability": flood_probability,
            "location": location,
            "country": country,
            "place": place,
            "mordecai_raw": self.original.get("mordecai_raw", None),
            "annotations_combined_model": self.original.get(
                "annotations_combined_model", None)
            })

    def set_flood_probability(self):
        try:
            flood_prob = self.original["annotations"]["flood_probability"]
        except (KeyError, AttributeError):
            raise MissingDataError("Record {} missing `flood_probability`!"\
                                   .format(self.original["id_str"]))

        assert isinstance(flood_prob, (Decimal, float, list)), \
            "Wrong type of flood_probability: must be float or list!"

        if isinstance(flood_prob, list):
            flood_prob = flood_prob[1] if flood_prob[0] == "yes" else 0
        self.normalized.update({"flood_probability": flood_prob})

    def set_country(self):
        country = self.normalized.get("country", None)
        if country is None:
            self.normalized["country"] = str(
                cc.getCountry(countries.Point(
                    self.normalized["location"]["lat"],
                    self.normalized["location"]["lon"]
                    )))
        place = self.normalized.get("place", None)
        if place is None:
            place = self.original.get("place", None)

            # Try to get place from tweet data. If it is available,
            # check the correctness of country, too.
            if isinstance(place, dict):
                self.normalized["place"] = place["name"]
                if place["country"] != self.normalized["country"]:
                    self.normalized["country"] = place["country"]
                return
            elif isinstance(place, str):
                self.normalized["place"] = place.strip()
                return

        # Try to get place from user's data.
        place = self.get_place_from_user()
        if place:
            self.normalized["place"] = place.strip()

    def get_locations_from_text(self, text):
        """
        Extracts locations from text and fills their geo-coordinates.

        :param text: polyglot.text.Text
        :return: list of dicts {
            "place": <str>,
            "location": {
                "lat": <float>,
                "lon": <float>
                }
            }
        """
        locations = []
        places = [" ".join(x) for x in text.entities if x.tag == "I-LOC"]
        for place in places:
            location = get_place_coords(place)
            if location:
                locations.append({
                    "place": place,
                    "location": location
                    })
        return locations

    def get_place_from_tweet(self):
        """
        Tries to get place from tweet record.

        :return: str
        """
        place = get_val_by_path("place/full_name", "place/name", **self.original)
        if place:
            return place.strip()

        return None

    def get_location_from_tweet(self):
        """
        Extracts geo-location in a tweet record.

        :return: dict {
            "lat": <float>,
            "lon": <float>
            }
        """
        # Simple point
        try:
            # Reverting coordinates as they're mixed up.
            location = {
                "lat": self.original["coordinates"]["coordinates"][1],
                "lon": self.original["coordinates"]["coordinates"][0]
                }
        except (KeyError, IndexError, TypeError):
            location = None
        if location:
            place = self.get_place_from_tweet()
            return {"place": place, "location": location}

        # Approximate point from bounding_box
        coords = get_val_by_path(
            'place/bounding_box/coordinates', 'location/geo/coordinates',
            **self.original
            )
        if coords:
            place = self.get_place_from_tweet()
            try:
                location = avg_coords_list(coords[0])
            except IndexError:
                location = avg_coords_list(coords)

            return {"place": place, "location": location}

        # Try to get coordinates from place name.
        place = self.get_place_from_tweet()
        if not place:
            return {}

        location = get_place_coords(place)
        if location:
            return {"place": place, "location": location}

        return {}

    def get_place_from_user(self):
        """
        Tries to get place from user's data.

        :return: str
        """
        place = get_val_by_path(
            "user/location",
            "user/derived/locations/locality",
            **self.original
            )
        if place:
            return place.strip()

        return None

    def get_location_from_user(self):
        """
        :return: dict {
            "place": <str>,
            "location": {
                "lat": <float>,
                "lon": <float>
                }
            }
        """
        place = self.get_place_from_user()
        if place:
            location = get_place_coords(place)
            if location:
                return {
                    "place": place,
                    "location": location
                    }
        return {}

    def set_geotag(self, text):
        """
        :param text: polyglot.text.Text
        """
        locations = self.get_locations_from_text(text)
        if len(locations) == 1:
            self.normalized.update(locations[0])
            return

        # Location obtained from tweet has higher priority.
        location_tweet = self.get_location_from_tweet()
        if not location_tweet:
            location_tweet = self.get_location_from_user()

        if len(locations) == 0:
            if location_tweet:
                self.normalized.update(location_tweet)
                return
            else:
                raise MissingDataError("Not enough data for geo-tagging tweet {}" \
                      .format(self.original["id_str"]))

        if len(locations) > 1:
            if location_tweet:
                # longest distance on Earth in meters
                distance = 20460000
                location = None
                for loc in locations:
                    dist_ = meters(loc["location"], location_tweet["location"])
                    if dist_ < distance:
                        location = loc
                        distance = dist_
            else:
                location = locations[0]

        self.normalized.update(location)

    def set_region(self):
        # TODO: administrative unit
        # https://drive.google.com/drive/folders/1mJV80c9xZS9RuogFS9oy59E43LaK-dvq
        pass

    def set_language(self, text):
        if text.language.code not in settings.LANGS:
            raise UnsupportedValueError(
                "Language '{}' is not supported!".format(text.language.code)
                )

        if text.language.code != self.normalized["lang"]:
            self.normalized.update({"lang": text.language.code})

    def get_timestamp(self):
        try:
            return timezone.datetime.fromtimestamp(
                int(self.original["timestamp_ms"])*0.001
                )
        except (TypeError, IndexError, ValueError):
            return timezone.now()

    def set_timestamp(self):
        try:
            created_at = get_parsed_datetime(self.original["created_at"])
        except TypeError:
            created_at = self.get_timestamp()

        if not created_at:
            raise UnsupportedValueError(
                "{} - field 'created_at' is wrongly formatted or empty: {}"\
                .format(self.original["id_str"], self.original["created_at"]))
        try:
            created_at = created_at.isoformat()
        except AttributeError:
            pass
        self.normalized.update({"created_at": created_at})

    def normalize(self, **kwargs):
        """
        :kwargs preserve_paths: list of str - path to values preserve
            (e.g. ['user/id', 'user/description']).
        :kwargs flatten: bool - if True (default), flattens the final
            structure.
        :kwargs exclude_from_flatten: list of field names. Ignored if
            `flatten` is False.

        :return: dict.
        """
        if "latlong" in self.original.keys():
            # OLD processing - delete after testing
            self.fill_annotations()
        else:
            # NEW processing
            self.normalized.update({"tweetid": self.original["id_str"]})
            text = Text(self.original["text"])
            self.set_language(text)
            self.set_geotag(text)
            self.set_country()
            self.set_region()
            self.set_flood_probability()
            self.set_timestamp()

        # Call prior to `self.restructure` to collect hashtags from all fields!
        hashtags, media_urls = [], []
        hashtags = collect_hashtags(self.original, hashtags)
        media_urls = collect_media_urls(self.original, media_urls)

        self.restructure(**kwargs)

        if kwargs.get("flatten", True):
            exclude = kwargs.get("exclude_from_flatten", [])
            exclude.extend(self.exclude_from_flatten)
            to_flatten = dict((k, v) for k, v in self.normalized.items()
                              if k not in exclude)
            to_flatten = flatten_dict(to_flatten)
            normalized = dict((k, v) for k, v in self.normalized.items()
                              if k in exclude)
            normalized.update(to_flatten)
            self.normalized = normalized

        # TODO: use polyglot for tokenization
        #       (self.normalized["text"].words -> clean stop-words)
        tokens = tokenize(self.normalized["text"],
                          self.normalized.get("lang", None))
        tokens.extend(hashtags)
        self.normalized.update({
            "tokens": list(set(tokens)),
            "media_urls": list(set(media_urls))
            })

        self.normalized = dict((key, val) for key, val in self.normalized.items()
                               if key in ES_INDEX_MAPPING["properties"].keys())
        return self.normalized


class ClusterBuilder(object):
    def __init__(self, *terms, **filters):
        """
        :terms: list of terms to group by.

        :filters: dict of {field_name: val} to filter query results.
            Special keys:
                  - 'search' is to search by keyword. The absense of
                    this key means {"match_all": {}}.
        """
        self.terms = terms
        match = filters.pop("search", None)
        self.match = self._get_match(match)
        self.raw_filters = filters.copy()
        self.filters = self._get_filters(**filters)
        self.query = {}
        self.errors = []

    @property
    def query(self):
        return self._query

    @property
    def filters(self):
        return self._filters

    @query.setter
    def query(self, match):
        self._query = match

    @filters.setter
    def filters(self, filters):
        self._filters = filters

    def _get_match(self, match=None):
        return QueryConverter(match).convert()

    def _get_filters(self, **filters):
        return FilterConverter(**filters).convert()

    def build_query(self, match=None, filters=None, size=settings.ES_MAX_RESULTS):
        match = match or self.match
        filters = filters or self.filters
        qry = {
            "query": {},
            "size": size
            }
        if filters:
            qry["query"].update({
                "bool": {
                    "must": match,
                    "filter": filters
                    }
                })
        else:
            qry["query"].update(match)

        return qry

    def get_clusters(self, normalize_text=True):
        self.query = self.build_query()
        self.query = self.define_aggregations()
        segments = self.get_segments(self.query)
        return {
            "clusters": self.collect_clusters(segments, normalize_text),
            "errors": self.errors
            }

    def build_aggregation(self):
        """
        Builds nested aggregations according to the order of self.terms.
        """
        aggregations = {}
        branch = aggregations
        for term in self.terms:
            assert term != settings.ES_GEO_FIELD, \
              "Cannot aggregate by {}, use GeoClusterBuilder for this purpose!".format(settings.ES_GEO_FIELD)

            name = "agg_{}".format(term)
            if term == settings.ES_TIMESTAMP_FIELD:
                agg = {
                    name: {
                        "date_histogram": {
                            "field": term,
                            "interval": self.raw_filters.get("interval", "5m")
                            }
                        }
                    }
            else:
                if term in ES_KEYWORDS:
                    term = "{}.keyword".format(term)
                agg = {
                    name: {
                        "terms": {
                            "field": term,
                            }
                        }
                    }
            branch.update({"aggregations": agg})
            branch = branch["aggregations"][name]

        return aggregations

    def define_aggregations(self, query=None):
        query = query or self.query
        query.update(self.build_aggregation())
        return query

    def _do_search(self, query):
        try:
            return search(query)
        except Exception as err:
            self.errors.append({
                "query": query,
                "error": err
                })
            return None

    def _buckets_to_segments(self, segments, buckets, chunk, term, agg_keys):
        for bucket in buckets:
            chunk[term] = bucket["key"]
            try:
                agg_key = [k for k in bucket.keys() if k in agg_keys][0]
            except IndexError:
                if bucket["doc_count"] >= settings.HOTSPOT_MIN_ENTRIES:
                    chunk_ = chunk.copy()
                    segments.append(chunk_)
            else:
                self._buckets_to_segments(
                    segments,
                    bucket[agg_key]["buckets"],
                    chunk,
                    agg_key.split("_")[1],
                    agg_keys
                    )
        return segments

    def get_segments(self, query):
        queryset = self._do_search(query)
        if queryset is None:
            return []

        segments = []
        chunk = {}
        keys = ["agg_{}".format(term) for term in self.terms]
        key = "agg_{}".format(self.terms[0])
        data = queryset["aggregations"][key]["buckets"]
        term = self.terms[0]
        return self._buckets_to_segments(segments, data, chunk, term, keys)

    def collect_clusters(self, segments, normalize_text):
        clusters = []
        for segment in segments:
            # Replace geo_bounding_box in self.filters with a box
            # that defines a current segment.
            segment_filters = self.raw_filters.copy()
            segment_filters.update(segment)
            segment_filters = self._get_filters(**segment_filters)
            query = self.build_query(filters=segment_filters)
            queryset = self._do_search(query)
            if queryset is None:
                continue

            # Cluster is a collection of more than N docs.
            if queryset["hits"]["total"] < settings.HOTSPOT_MIN_ENTRIES:
                continue

            docs = []
            for doc in queryset["hits"]["hits"]:
                # Retain only fields necessary for text analysis.
                doc = RecordDict(
                    _id=doc["_id"],
                    text=doc["_source"]["text"],
                    tokens=doc["_source"]["tokens"]
                    )
                if normalize_text:
                    doc.update(_normalized_text=normalize_aggressive(doc.text))
                docs.append(doc)
            segment.update({"docs": docs})
            clusters.append(segment)

        return clusters

    def get_clusters(self, normalize_text=True):
        self.query = self.build_query()
        self.query = self.define_aggregations()
        segments = self.get_segments(self.query)
        self.clusters = self.collect_clusters(segments, normalize_text)
        return RecordDict(clusters=self.clusters, errors=self.errors)


class GeoClusterBuilder(ClusterBuilder):
    def __init__(self, *terms, **filters):
        """
        :terms: list of terms to group by (in addition to 'location')

        :filters: dict of {field_name: val} to filter query results.
            Special keys:
                  - 'search' is to search by keyword. The absense of
                    this key means {"match_all": {}}.
                  - 'precision' defines geohash precision (by default 5,
                    i.e. the least accurate for getting bigger clusters).
        """
        super().__init__(*terms, **filters)
        self.precision = filters.pop("precision", 5)
        self.segments_fieldname = "segments"

    def build_aggregation(self):
        aggs = {
            "cell": {
                "geo_bounds": {
                    "field": settings.ES_GEO_FIELD
                    }
                }
            }
        for term in self.terms:
            key = "doc_count_{}".format(term)
            if term in ES_KEYWORDS:
                term = "{}.keyword".format(term)
            aggs.update({key: {"terms": {"field": term}}})

        return {
            "aggregations": {
                self.segments_fieldname: {
                    "geohash_grid": {
                        "field": settings.ES_GEO_FIELD,
                        "precision": self.precision,
                        },
                    "aggs": aggs
                    }
                }
            }

    def _check_lat_long(self, box):
        """
        Artificially extends geo point to a small box.
        """
        if box["top_left_lat"] == box["bottom_right_lat"]:
            box["top_left_lat"] += 0.001
            box["bottom_right_lat"] -= 0.001
        if box["top_left_lon"] == box["bottom_right_lon"]:
            box["top_left_lon"] -= 0.001
            box["bottom_right_lon"] += 0.001
        return box

    def collect_clusters(self, segments, normalize_text):
        """
        Adapted to segment by geo-location.
        """
        clusters = []
        for segment in segments:
            # Replace geo_bounding_box in self.filters with a box
            # that defines a current segment.
            segment_filters = self.raw_filters.copy()
            segment_filters.update(segment)
            segment_filters = self._get_filters(**segment_filters)
            query = self.build_query(filters=segment_filters)
            queryset = self._do_search(query)
            if queryset is None:
                continue

            # Cluster is a collection of more than N docs.
            if queryset["hits"]["total"] < settings.HOTSPOT_MIN_ENTRIES:
                continue

            docs = []
            for doc in queryset["hits"]["hits"]:
                # Retain only fields necessary for text analysis.
                doc = RecordDict(
                    _id=doc["_id"],
                    text=doc["_source"]["text"],
                    tokens=doc["_source"]["tokens"]
                    )
                if normalize_text:
                    doc.update(_normalized_text=normalize_aggressive(doc.text))
                docs.append(doc)
            segment.update({"docs": docs})
            clusters.append(segment)

        return clusters

    def _buckets_to_segments(self, buckets):
        """
        Converts an ES buckets format to plain list of geo-cells.
        """
        segments = []
        for bucket in buckets:
            local_filter = flatten_dict(bucket["cell"]["bounds"])
            local_filter = self._check_lat_long(local_filter)
            for term in self.terms:
                term_name = "doc_count_{}".format(term)
                for buck in bucket[term_name]["buckets"]:
                    if buck["doc_count"] < settings.HOTSPOT_MIN_ENTRIES:
                        continue

                    local_flt = local_filter.copy()
                    local_flt.update({term: buck["key"]})
                    segments.append(local_flt)

        return segments

    def get_segments(self, query):
        queryset = self._do_search(query)
        if queryset is None:
            return []

        return self._buckets_to_segments(
            queryset["aggregations"][self.segments_fieldname]["buckets"]
            )
