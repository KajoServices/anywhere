import json
import dpath.util
from collections import MutableMapping
from decimal import Decimal

import settings.base as conf
from dataman.elastic import tokenize, ES_INDEX_MAPPING
from core.utils import RecordDict, get_val_by_path, flatten_dict

from countries import countries
cc = countries.CountryChecker(conf.WORLD_BORDERS)


def extract_hatshtags(val):
    tags = []
    if isinstance(val, str):
        tags = [x.strip("#.,-\"\'&*^!") for x in val.split()
                if (x.startswith("#") and len(x) < 256)]
    elif isinstance(val, list):
        for entity in val:
            if not isinstance(entity, dict):
                continue

            if 'hashtags' in entity:
                try:
                    tags.extend(entity['hashtags'])
                except:
                    pass
            else:
                try:
                    tags = [x['text'].strip() for x in val]
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
                    _media_urls.append(item['media_url'])
                except (KeyError, TypeError):
                    pass
                try:
                    _media_urls.append(item['media_url_https'])
                except (KeyError, TypeError):
                    pass
        else:
            continue

        media_urls.extend(_media_urls)
    return list(set(media_urls))


class TweetNormalizer(object):
    preserve_paths = [
        'id', 'tweetid', 'text', 'lang', 'created_at', 'ttype', 'annotations',
        'geotags', 'latlong', 'annotations_combined_model', 'mordecai_raw',
        'geo', 'coordinates', 'place', 'media_urls',
        'user/id', 'user/name', 'user/screen_name', 'user/location',
        'user/description', 'user/followers_count', 'user/friends_count',
        'user/listed_count', 'user/favourites_count', 'user/statuses_count',
        'user/created_at', 'user/utc_offset', 'user/time_zone',
        'user/lang', 'user/profile_image_url',
        ]
    exclude_from_flatten = ['location']

    def __init__(self, doc, **kwargs):
        self.original = doc
        assert isinstance(self.original['tweet'], (dict, str)), \
            "Wrong type: must be string or dict"
        if isinstance(self.original['tweet'], str):
            try:
                self.original['tweet'] = json.loads(self.original['tweet'])
            except (TypeError, ValueError):
                raise

        self.normalized = self.original['tweet'].copy()

    def restructure(self, **kwargs):
        """
        Leaves in place  only elements by given paths.

        :param preserve_paths: list of str - fields to preserve
            (field names and paths).
        :return: dict.
        """
        paths = kwargs.get('preserve_paths', [])
        paths.extend(self.preserve_paths)
        paths = list(set(paths))

        subtrees = {}
        for path in paths:
            val = get_val_by_path(path, **self.normalized)
            if not '/' in path:
                self.normalized[path] = val
            else:
                dpath.util.new(subtrees, path, val)

        self.normalized.update(subtrees)

    def fill_annotations(self):
        try:
            annot = self.original['annotations']
        except KeyError:
            raise Exception("Record must contain 'annotations'!")

        try:
            flood_probability = annot['flood_probability']
        except (KeyError, AttributeError):
            raise Exception("Record annotations must contain `flood_probability`!")
        assert isinstance(flood_probability, (Decimal, float, list)), "Wrong type: must be float or list!"
        if isinstance(flood_probability, list):
            flood_probability = flood_probability[1] if flood_probability[0] == 'yes' else 0

        try:
            location = {
                "lat": self.original['latlong']['lat'],
                "lon": self.original['latlong']['long']
                }
        except TypeError:
            location = {
                "lat": self.original['latlong'][0],
                "lon": self.original['latlong'][1]
                }
        except KeyError:
            raise Exception("Record must contain 'latlong'!")

        try:
            geotags = self.original['geotags']
        except:
            country = None
            place = None
        else:
            country = geotags.get('country_predicted', None)
            place = geotags.get('place_name', None)

        self.normalized.update({
            'tweetid': self.original['tweetid'],
            'created_at': self.original['created_at'],
            'lang': self.original['lang'],
            'flood_probability': flood_probability,
            'location': location,
            'country': country,
            'place': place,
            'mordecai_raw': self.original.get('mordecai_raw', None),
            'annotations_combined_model': self.original.get(
                'annotations_combined_model', None)
            })

    def ensure_place(self):
        country = self.normalized.get('country', None)
        if country is None:
            self.normalized['country'] = str(
                cc.getCountry(
                    countries.Point(
                        self.normalized['location']['lat'],
                        self.normalized['location']['lon']
                        )
                    )
                )
        place = self.normalized.get('place', None)
        if place is None:
            place = self.original['tweet'].get('place', None)

            # Try to get place from tweet data. If it is available,
            # check the correctness of country, too.
            if isinstance(place, dict):
                self.normalized['place'] = place['name']
                if place['country'] != self.normalized['country']:
                    self.normalized['country'] = place['country']
                return
            elif isinstance(place, str):
                self.normalized['place'] = place.strip()
                return

        # Try to get place from user's data.
        place = get_val_by_path(
            'user/location',
            'user/derived/locations/locality',
            **self.original['tweet'])
        if place:
            self.normalized['place'] = place.strip()

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
        self.fill_annotations()
        self.ensure_place()

        # This should be called before `self.restructure` to collect
        # hashtags from all fields!
        hashtags, media_urls = [], []
        hashtags = collect_hashtags(self.original['tweet'], hashtags)
        media_urls = collect_media_urls(self.original['tweet'], media_urls)

        self.restructure(**kwargs)

        if kwargs.get('flatten', True):
            exclude = kwargs.get('exclude_from_flatten', [])
            exclude.extend(self.exclude_from_flatten)
            to_flatten = dict((k, v) for k, v in self.normalized.items()
                              if k not in exclude)
            to_flatten = flatten_dict(to_flatten)
            normalized = dict((k, v) for k, v in self.normalized.items()
                              if k in exclude)
            normalized.update(to_flatten)
            self.normalized = normalized

        tokens = tokenize(self.normalized['text'],
                          self.normalized.get('lang', None))
        tokens.extend(hashtags)
        self.normalized['tokens'] = list(set(tokens))
        self.normalized['media_urls'] = list(set(media_urls))

        self.normalized = dict((key, val) for key, val in self.normalized.items()
                               if key in ES_INDEX_MAPPING['properties'].keys())
        return self.normalized
