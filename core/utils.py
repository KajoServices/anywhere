import time
import dpath.util
from collections import MutableMapping


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


def flatten_dict(dict_, parent_key='', separator='_'):
    items = []
    for key, val in dict_.items():
        new_key = '{0}{1}{2}'.format(parent_key, separator, key) if parent_key else key
        if isinstance(val, MutableMapping):
            items.extend(
                flatten_dict(val, new_key, separator=separator).items()
                )
        else:
            items.append((new_key, val))
    return dict(items)
