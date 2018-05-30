import re

from elasticsearch import Elasticsearch, NotFoundError

import settings.base as conf
from core.utils import get_val_by_path


ES_INDEX = conf.ES_INDEX
ES_DOC_TYPE = conf.ES_DOC_TYPE
es = Elasticsearch(
    [conf.ES_ADDRESS],
    port=conf.ES_PORT,
    timeout=30,
    max_retries=10,
    retry_on_timeout=True
    )

ES_INDEX_MAPPING = {
    'properties': {
        'created_at': {
            'type': 'date'
        },
        'flood_probability': {
            'type': 'float'
        },
        'location': {
            'type': 'geo_point'
        },
        'text': {
            'type': 'text',
        },
        'user_id': {
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
        'tweetid': {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        'flood_probability': {
            "type": "float"
        },
        'country': {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        'text': {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        'lang': {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        'place': {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        'tokens': {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        'media_urls': {
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


def create_index(mapping):
    response = es.indices.create(index=ES_INDEX, body=mapping)
    return response


def put_mapping(body):
    response = es.indices.put_mapping(
        index=ES_INDEX, doc_type=ES_DOC_TYPE, body=body
        )
    return response


def ensure_mapping():
    body = {'mappings': {ES_DOC_TYPE: ES_INDEX_MAPPING}}
    try:
        mapping = es.indices.get_mapping(index=ES_INDEX, doc_type=ES_DOC_TYPE)
    except NotFoundError:
        mapping = create_index(body)
    else:
        if mapping[ES_INDEX] != body:
            try:
                mapping = put_mapping(ES_INDEX_MAPPING)
            except Exception as err:
                print('! [ensure_mapping] {}: {}'.format(type(err), err))
    return mapping


def create_or_update_index(id_, body):
    response = es.index(index=ES_INDEX, doc_type=ES_DOC_TYPE, id=id_, body=body)
    return response['result']


def delete_from_index(id_):
    response = es.delete(index=ES_INDEX, doc_type=ES_DOC_TYPE, id=id_)
    return response['result']


def scroll(scroll_id):
    try:
        response = es.scroll(scroll_id=scroll_id, scroll='1m')
    except Exception as exc:
        return None
    else:
        return response


def search(query, scroll=False):
    try:
        if scroll:
            response = es.search(
                index=ES_INDEX,
                doc_type=ES_DOC_TYPE,
                body=query,
                scroll='1m'
                )
        else:
            response = es.search(
                index=ES_INDEX,
                doc_type=ES_DOC_TYPE,
                body=query
                )
    except NotFoundError:
        return None
    else:
        return response


def search_id(id_):
    query = {'query': {'match' : {'_id': id_}}}
    res = search(query)
    if res is None:
        return None

    if res['hits']['total'] == 0:
        return None

    return res


def return_all(size=100):
    return search({"query": {"match_all": {}}, 'size': size})


def termvectors(_id, **kwargs):
    return es.termvectors(
        index=ES_INDEX,
        doc_type=ES_DOC_TYPE,
        id=_id,
        **kwargs
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
        return rec['coordinates']
    except KeyError:
        pass

    src = get_val_by_path(
        'place/bounding_box/coordinates',
        'location/geo/coordinates',
        **rec
        )
    # Approximate point from list of points
    try:
        return geo_approximate_place(src[0])
    except IndexError:
        return geo_approximate_place(rec)

    return []


def analyze_text(text, lang='en'):
    analyzers = {
        'en': 'english',
        'es': 'spanish',
        'fr': 'french',
        'it': 'italian'
        }
    try:
        analyzer = analyzers[lang]
    except KeyError:
        analyzer = 'standard'
    body = {
        'filter' : ['lowercase'],
        'analyzer' : analyzer,
        'text' : text
        }
    resp = es.indices.analyze(ES_INDEX, body=body)
    return [x['token'] for x in resp['tokens']]


def clean_tweet_text(text):
    urls = re.findall("(?P<url>https?://[^\s]+)", text)
    usernames = ['@'+u for u in re.findall("@([a-z0-9_]+)", text, re.I)]
    for word in urls + usernames:
        text = text.replace(word, '')
    return text.strip()


def tokenize(text, lang='en'):
    text = clean_tweet_text(text)
    tokens = analyze_text(text, lang)

    # Remove repeated items.
    tokens = list(set(tokens))

    # Remove single characters and twitter-specific strings
    not_allowed = ['rt', 'http', 'https', 'ftp']
    tokens = [
        t for t in tokens
        if (t.lower().strip() not in not_allowed)
        and (len(t.lower().strip()) > 1)
        ]

    # TODO:
    #     - remove adverbs, prepositions, etc.
    return tokens
