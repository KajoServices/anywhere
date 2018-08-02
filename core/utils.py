import os
import re
import json
import random
import collections
from string import ascii_lowercase, digits
from datetime import datetime, date, time, timedelta
from tempfile import NamedTemporaryFile
from geopy.geocoders import Nominatim
from geopy.distance import distance

import dpath.util
import dateparser

from django.conf import settings
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime
from django.contrib.gis.geos import GEOSGeometry


GEO_LOCATOR = Nominatim(user_agent="python")
CHARS = ascii_lowercase + digits
TS_GTE = settings.ES_TIMESTAMP_FIELD + '__gte'
TS_LTE = settings.ES_TIMESTAMP_FIELD + '__lte'
QUERY_TERMS = [
    'ne', 'lt', 'lte', 'gt', 'gte', 'not', 'in', 'nin', 'mod', 'all', 'size',
    'exists', 'exact', 'iexact', 'contains', 'icontains', 'startswith',
    'istartswith', 'endswith', 'iendswith', 'match'
    ]


class MalformedValueError(Exception):
    pass


class UnsupportedValueError(Exception):
    pass


class MissingDataError(Exception):
    pass


class TempFile(object):
    def __init__(self, data='', **kwargs):
        self._f = NamedTemporaryFile(delete=False, **kwargs)
        self._f.write(data)
        self._f.close()

    def __enter__(self):
        return self._f.name

    def __exit__(self, *args):
        os.unlink(self._f.name)


class RecordDict(dict):
    """
    Dictionary that acts like a class with keys accessed as attributes.
    `inst['foo']` and `inst.foo` is the same.
    """
    def __init__(self, **kwargs):
        super(RecordDict, self).__init__(**kwargs)
        self.__dict__ = self

    def exclude(self, *args):
        for key in args:
            del self[key]
        return self

    @classmethod
    def from_list(cls, container, key, val):
        kwargs = dict((s[key], s[val]) for s in container)
        return cls(**kwargs)


def rand_string(size=6):
    """
    Generates quazi-unique sequence from random digits and letters.
    """
    return ''.join(random.choice(CHARS) for x in range(size))


def _clean(line):
    return re.sub(r'\W+', '_', line)


def ensure_dict(obj):
    assert isinstance(obj, (dict, str)), \
        "Wrong type: must be string or dict"
    if isinstance(obj, str):
        return json.loads(obj)

    return obj


def ensure_tmp_dir():
    """
    Ensures project _tmp directory.
    Returns _tmp dir name.
    """
    if (not os.path.isdir(settings.TEMP_ROOT)) \
      or (not os.path.exists(settings.TEMP_ROOT)):
        os.makedirs(settings.TEMP_ROOT)
    return settings.TEMP_ROOT


def get_val_by_path(*args, **kwargs):
    """
    Successively tries to get a value by paths from args,
    and returns it if successful, or empty string, otherwise.

    :args: list of strings
    :kwargs: dict (original document)
    :return: object or None
    """
    for path in args:
        try:
            val = dpath.util.get(kwargs, path)
        except KeyError:
            continue
        else:
            return val
    return None


def flatten_list(list_):
    return [item for sublist in list_ for item in sublist]


def flatten_dict(dict_, parent_key='', separator='_'):
    items = []
    for key, val in dict_.items():
        new_key = '{0}{1}{2}'.format(parent_key, separator, key) if parent_key else key
        if isinstance(val, collections.MutableMapping):
            items.extend(
                flatten_dict(val, new_key, separator=separator).items()
                )
        else:
            items.append((new_key, val))
    return dict(items)


def deep_update(source, overrides):
    """
    Updates a nested dictionary or similar mapping.
    Modifies `source` in place.
    """
    for key, value in overrides.items():
        if isinstance(value, collections.Mapping) and value:
            returned = deep_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = overrides[key]

    return source


def get_tz(tz=None):
    """
    Converts `tz` into pytz object.
    If `tz` is None, uses settings.TIME_ZONE.

    :param tz: - string representing timezone or a pytz object.
    :return: pytz object.
    """
    if tz is None:
        tz = settings.TIME_ZONE
    if isinstance(tz, str):
        return timezone.pytz.timezone(tz)
    else:
        return tz


def get_parsed_datetime(value):
    """
    Parses string datetime or date into timezone.datetime.
    """
    assert isinstance(value, str), "Value is not a string: %s" % value
    dt = parse_datetime(value)
    if dt is None:
        dt = parse_date(value)
        dt = timezone.datetime.combine(dt, timezone.datetime.min.time())
    return dt


def localize_timestamp(timestamp, tz=None):
    """
    Localizes timestamp to a given timezone
    If none of those have worked, localizes to settings.TIME_ZONE.
    """
    if isinstance(timestamp, str):
        timestamp = get_parsed_datetime(timestamp)
    time_zone = get_tz(tz)
    try:
        local_time = timezone.localtime(timestamp, time_zone)
    except (ValueError, OverflowError):
        local_time = timestamp.replace(tzinfo=time_zone)
    return local_time, time_zone.zone


def convert_time_range(trange, tz=None):
    """
    Converts freeform time range into a tuple of localized
    timestamps (start, end).

    If `tz` is None, uses settings.TIME_ZONE for localizing
    time range.

    :param trange: - string representing time-range. The options
        are:
        * string in format 'x1|x2', where x1 and x2 are start
          and end date in the format YYYYmmdd[THH:MM:SS.mmmmm] 
          (in fact, any other format would work well, the function
          tries its best to determine format and parse timestamps)

        * string in format 'x1|x2', where x1 and x2 are given in
          human readable format, as described in the dateparser doc:
          (see https://github.com/scrapinghub/dateparser)

        * one of the following keywords:
          'today',  'yesterday', 'this week', 'last week',
          'this month', 'last month', 'this year', 'last year'

    :param tz: - timezone (optional). Either string representing
        a timezone (e.g. "America/Lima") or a pytz object.

    :return: tuple of two TZ-aware timestamps.
    """
    # Form time range as a tuple of naive datetimes.
    assert isinstance(trange, str), "Value is not a string: %s" % trange
    trange = trange.strip().lower()
    _time = lambda d: datetime.combine(d, time())
    today = date.today()
    if trange == 'today':
        ts_from = _time(today)
        ts_to = ts_from + timedelta(days=1, seconds=-1)
    elif trange == 'yesterday':
        ts_from = _time(today+timedelta(days=-1))
        ts_to = ts_from + timedelta(days=1, seconds=-1)
    elif trange == 'this week':
        ts_from = _time(today-timedelta(days=today.weekday()))
        ts_to = ts_from + timedelta(days=7, seconds=-1)
    elif trange == 'last week':
        this_week = _time(today-timedelta(days=today.weekday()))
        ts_to = this_week + timedelta(seconds=-1)
        ts_from = _time(ts_to - timedelta(days=ts_to.weekday()))
    elif trange == 'this month':
        ts_from = _time(today.replace(day=1))
        next_month = ts_from.replace(day=28) + timedelta(days=4)
        this_month_last_day = next_month - timedelta(days=next_month.day)
        ts_to = this_month_last_day + timedelta(days=1, seconds=-1)
    elif trange == 'last month':
        ts_to = _time(today.replace(day=1)) + timedelta(seconds=-1)
        ts_from = _time(ts_to.replace(day=1))
    elif trange == 'this year':
        ts_from = _time(today.replace(month=1, day=1))
        this_year_last_day = _time(today.replace(month=12, day=31))
        ts_to = this_year_last_day + timedelta(days=1, seconds=-1)
    elif trange == 'last year':
        ts_to = _time(today.replace(month=1, day=1)) + timedelta(seconds=-1)
        ts_from = _time(ts_to.replace(month=1, day=1))
    else:
        try:
            ts_from, ts_to = [dateparser.parse(t) for t in trange.split('|')]
        except ValueError:
            raise MalformedValueError(
                'Cannot parse datetime range: wrong format!\n' + \
                'Datetime range should be two date[time] values divided by vertical bar (|)'
                )
        if (ts_from is None) or (ts_to is None):
            raise MalformedValueError('Cannot parse datetime range: wrong format!')
        # Stretch date values (without time) to the end of day
        # (ignore microseconds).
        if ts_to.minute == 0 and ts_to.second == 0:
            ts_to += timedelta(days=1, seconds=-1)

    # Figure out desired timezone.
    time_zone = get_tz(tz)

    # Add timezone info to the result.
    ts_from = ts_from.replace(tzinfo=time_zone)
    ts_to = ts_to.replace(tzinfo=time_zone)
    if ts_from > ts_to:
        raise MalformedValueError(
            'Start date cannot be greater than the end date!'
            )
    return (ts_from, ts_to)


def build_filters_time(filters):
    """
    Re-defines filters dict, taiking into account custom timestamp
    filters: all keys in `filters` that contain ES_TIMESTAMP_FIELD
    are being converted into the following form:
    {
        "range": {
            "ES_TIMESTAMP_FIELD": {
                "gte": <ISO datetime>,
                "lte": <ISO datetime>,
            }
        }
    }

    :param filters: dict
    :return: dict - modified filters.
    """
    def filter_exact(val):
        ts, _ = localize_timestamp(val)
        return {
            TS_GTE: ts,
            TS_LTE: ts + timezone.timedelta(minutes=1)
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
            _ = get_parsed_datetime(val)
        except TypeError:
            ts_from, ts_to = convert_time_range(val)
            result.update({TS_GTE: ts_from, TS_LTE: ts_to})
        else:
            # The same as {ES_TIMESTAMP_FIELD}__exact.
            result.update(filter_exact(val))

    timestamp_range = {}
    for key, val in result.items():
        name = key.split('__')[1]
        timestamp_range.update({name: val.isoformat()})
    if timestamp_range:
        timestamp_range = {"range": {settings.ES_TIMESTAMP_FIELD: timestamp_range}}

    return timestamp_range


def build_filters_geo(filters):
    if not all([k in filters for k in settings.ES_BOUNDING_BOX_FIELDS]):
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


def avg_coords(rec):
    lon, lat = 0, 0
    count = float(len(rec))
    for each in rec:
        lon += rec[each]['lon']
        lat += rec[each]['lat']
    return {"lat": lat/count, "lon": lon/count}


def avg_coords_list(coords):
    """
    :param coords: list of lists or tuples [(lon1, lat1), ...(lonN, latN)]
    :return: dict {lat: xxx, lon: yyy}
    """
    count = float(len(coords))
    lon, lat = 0, 0
    for x in coords:
        lon += x[0]
        lat += x[1]
    return {"lat": lat/count, "lon": lon/count}


def get_place_coords(place):
    """
    Figures out geo-coords for a given place.

    :param place: str.
    :return: dict {lat: <float>, lon: <float>} or empty dict, if unsuccessfull.
    """
    coords = {}
    geo_location = GEO_LOCATOR.geocode(place)
    try:
        coords.update({
            "lat": geo_location.latitude,
            "lon": geo_location.longitude
            })
    except (ValueError, AttributeError):
        pass

    return coords


def geo(lat, lon, srid=4326):
    return GEOSGeometry('POINT(%.6f %.6f)' % (lon, lat), srid=srid)


def meters(point_from, point_to):
    """
    Distance in meters between two points.

    :param point_from: dict {lat: <float>, lon: <float>}
    :param point_to: the same
    :return: float
    """
    point_from = geo(point_from["lat"], point_from["lon"])
    point_to = geo(point_to["lat"], point_to["lon"])
    return distance(point_from, point_to).m
