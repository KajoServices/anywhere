import re

from django.conf import settings
from django.db.models.constants import LOOKUP_SEP

from elasticsearch import NotFoundError

from core.utils import get_val_by_path, build_filters_geo, build_filters_time, \
     QUERY_TERMS


es = settings.ES_CLIENT


ES_INDEX_MAPPING = {
    "properties": {
        "created_at": {
            "type": "date"
        },
        "flood_probability": {
            "type": "float"
        },
        "location": {
            "type": "geo_point"
        },
        "text": {
            "type": "text",
        },
        "user_id": {
            "type": "long"
        },
        "user_created_at": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_description": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_favourites_count": {
            "type": "long"
        },
        "user_followers_count": {
            "type": "long"
        },
        "user_friends_count": {
            "type": "long"
        },
        "user_listed_count": {
            "type": "long"
        },
        "user_statuses_count": {
            "type": "long"
        },
        "user_lang": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_location": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_name": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_profile_image_url": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_time_zone": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "user_utc_offset": {
            "type": "long"
        },
        "tweetid": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "flood_probability": {
            "type": "float"
        },
        "country": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "text": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "lang": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "place": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "representative": {
            "type": "boolean"
        },
        "tokens": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "media_urls": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        }
    }
}

ES_KEYWORDS = [
    key for key, mp in ES_INDEX_MAPPING["properties"].items()
    if get_val_by_path("fields/keyword/type", **mp) == "keyword"
    ]


def create_index(mapping):
    response = es.indices.create(index=settings.ES_INDEX, body=mapping)
    return response


def put_mapping(body):
    response = es.indices.put_mapping(
        index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE, body=body
        )
    return response


def ensure_mapping():
    body = {"mappings": {settings.ES_DOC_TYPE: ES_INDEX_MAPPING}}
    try:
        mapping = es.indices.get_mapping(
            index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE
            )
    except NotFoundError:
        mapping = create_index(body)
    else:
        if mapping[settings.ES_INDEX] != body:
            try:
                mapping = put_mapping(ES_INDEX_MAPPING)
            except Exception as err:
                print("! [ensure_mapping] {}: {}".format(type(err), err))
    return mapping


def index_required(method):
    """
    Profiling decorator, measures function runing time.
    """
    def index_required_wrapper(*args, **kwargs):
        try:
            result = method(*args, **kwargs)
        except NotFoundError:
            # `ensure_mapping` includes index creation.
            ensure_mapping()
            result = method(*args, **kwargs)
        return result
    return index_required_wrapper


def _do_create_or_update_doc(id_, body):
    return es.index(
        index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE,
        id=id_, body=body
        )


@index_required
def create_or_update_doc(id_, body):
    response = _do_create_or_update_doc(id_, body)
    return response["result"]


@index_required
def delete_doc(id_):
    response = es.delete(
        index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE, id=id_
        )
    return response["result"]


@index_required
def search(query, scroll=False):
    try:
        if scroll:
            response = es.search(
                index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE,
                body=query, scroll="1m"
                )
        else:
            response = es.search(
                index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE,
                body=query
                )
    except NotFoundError:
        return None
    else:
        return response


@index_required
def scroll(scroll_id):
    try:
        response = es.scroll(scroll_id=scroll_id, scroll="1m")
    except Exception as exc:
        return None
    else:
        return response


def search_id(id_):
    query = {"query": {"match" : {"_id": id_}}}
    res = search(query)
    if res is None:
        return None

    if res["hits"]["total"] == 0:
        return None

    return res


@index_required
def update_doc(id_, **data):
    res = search_id(id_)
    doc = res["hits"]["hits"][0]["_source"]
    doc.update(data)
    response = create_or_update_doc(id_, doc)
    return response


def return_all(size=settings.ES_MAX_RESULTS):
    return search({"query": {"match_all": {}}, "size": size})


@index_required
def termvectors(_id, **kwargs):
    return es.termvectors(
        index=settings.ES_INDEX, doc_type=settings.ES_DOC_TYPE,
        id=_id, **kwargs
        )


def delete_index(index_name):
    response = es.indices.delete(index=index_name, ignore=[400, 404])
    return response


def geo_approximate_place(coords):
    """
    Approximates central point from the list of geo coords

    :param coords: list of lists (or tuples)
    :return: list
    """
    latitudes, longitudes = 0, 0
    for x in coords:
        latitudes += x[1]
        longitudes += x[0]
    return [latitudes/len(coords), longitudes/len(coords)]


def get_coords(rec):
    """
    Returns a geo-point in the format [lng, lat].

    :param rec: dict
    :return: list
    """
    # Simple point
    try:
        # Reverting coordinates as they're mixed up.
        return rec["coordinates"]
    except KeyError:
        pass

    src = get_val_by_path(
        "place/bounding_box/coordinates",
        "location/geo/coordinates",
        **rec
        )
    # Approximate point from list of points
    try:
        return geo_approximate_place(src[0])
    except IndexError:
        return geo_approximate_place(rec)

    return []


@index_required
def analyze_text(text, lang="en"):
    analyzers = {
        "en": "english",
        "es": "spanish",
        "fr": "french",
        "it": "italian"
        }
    try:
        analyzer = analyzers[lang]
    except KeyError:
        analyzer = "standard"
    body = {
        "filter" : ["lowercase"],
        "analyzer" : analyzer,
        "text" : text
        }
    resp = es.indices.analyze(settings.ES_INDEX, body=body)
    return [x["token"] for x in resp["tokens"]]


def clean_tweet_text(text):
    urls = re.findall("(?P<url>https?://[^\s]+)", text)
    usernames = ['@'+u for u in re.findall("@([a-z0-9_]+)", text, re.I)]
    for word in urls + usernames:
        text = text.replace(word, '')
    return text.strip()


def tokenize(text, lang="en"):
    # TODO:
    #     - remove adverbs, prepositions, etc.
    text = clean_tweet_text(text)
    tokens = analyze_text(text, lang)

    # Remove repeated items.
    tokens = list(set(tokens))

    # Remove single characters and twitter-specific strings
    not_allowed = ["rt", "http", "https", "ftp"]
    tokens = [
        t for t in tokens
        if (t.lower().strip() not in not_allowed)
        and (len(t.lower().strip()) > 1)
        ]
    return tokens


class QueryConverter(object):
    """
    A provisioner of a search query.
    """
    def __init__(self, query=None):
        """
        :param query: string - search query
        """
        self.query = query

    def convert(self):
        if not self.query:
            return {"match_all": {}}

        return {"match": self.query}


class FilterConverter(object):
    def __init__(self, **filters):
        """
        :param index: string - index name
        :param doc_type: string - doc_type in the specified index name
        :kwargs filters: dict - actual filters
        """
        self.input_filters = filters
        self.keywords = ES_KEYWORDS
        self.schema = ES_INDEX_MAPPING["properties"]

    def fill_keywords(self, keywords=None):
        if not keywords:
            keywords = []
        self.keywords.extend(keywords)
        self.keywords = list(set(self.keywords))

    def fill_schema(self, schema=None):
        if schema:
            self.schema = schema

    def get_exist_filters(self):
        return [
            {"exists": {"field": settings.ES_TIMESTAMP_FIELD}},
            {"exists": {"field": settings.ES_GEO_FIELD}},
            ]

    def convert(self, schema=None, keywords=None):
        if not self.input_filters:
            return {}

        es_filters = []
        es_filters.extend(self.get_exist_filters())
        self.fill_keywords(keywords)
        self.fill_schema(schema)

        filters_geo = build_filters_geo(self.input_filters)
        if filters_geo:
            es_filters.append(filters_geo)

        filters_time = build_filters_time(self.input_filters)
        if filters_time:
            es_filters.append(filters_time)

        for filter_expr, value in self.input_filters.items():
            filter_bits = filter_expr.split(LOOKUP_SEP)
            field_name = filter_bits.pop(0)

            # Ignore fields we know nothing about.
            if field_name not in self.schema.keys():
                continue

            # Those are used already in build_filters_time and build_filters_geo.
            if (field_name == settings.ES_TIMESTAMP_FIELD) or \
                (field_name in settings.ES_BOUNDING_BOX_FIELDS):
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
                if field_name in self.keywords:
                    field_name = "{}.keyword".format(field_name)

                es_filters.append({
                    "term": {
                        field_name: value
                        }
                    })
        return es_filters
