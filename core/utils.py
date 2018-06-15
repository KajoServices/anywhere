import time
import random
import dpath.util
import collections
from string import ascii_lowercase, digits

from django.conf import settings
from django.utils import timezone
from django.utils.dateparse import parse_date, parse_datetime

CHARS = ascii_lowercase + digits


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


def timeit(method):
    """
    Profiling decorator, measures function runing time.
    """
    def timeit_wrapper(*args, **kwargs):
        time_started = time.time()
        result = method(*args, **kwargs)
        time_ended = time.time()
        time_sec = time_ended - time_started
        print('%s\t%2.2fmin\t%2.8fs\t%sms' % (
            method.__name__,
            time_sec / 60,
            time_sec,
            time_sec * 1000))
        return result
    return timeit_wrapper


def rand_string(size=6):
    """
    Generates quazi-unique sequence from random digits and letters.
    """
    return ''.join(random.choice(CHARS) for x in range(size))


def get_val_by_path(*args, **kwargs):
    """
    Successively tries to get a value by paths from args,
    and returns it if successful, or empty string, otherwise.

    :args: list of strings
    :kwargs: dict (original document)
    :return: str
    """
    for path in args:
        try:
            val = dpath.util.get(kwargs, path)
        except KeyError:
            continue
        else:
            return val
    return ''


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
