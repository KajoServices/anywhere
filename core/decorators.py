import time

from .utils import rand_string


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


def hashtag(method, size=6):
    """
    Adds quazi-unique sequence to the end of the output of function.
    """
    def hash_tag_wrapper(*args, **kwargs):
        result = method(*args, **kwargs)
        hash_tag = rand_string(size)
        return '%s_%s' % (result, hash_tag)
    return hash_tag_wrapper


def timetag(method):
    """
    Tag the output of function with current time in seconds from epoch, rounded.
    """
    def time_tag_warpper(*args, **kwargs):
        result = method(*args, **kwargs)
        time_tag = str(int(time.time()))
        return '%s_%s' % (result, time_tag)
    return time_tag_warpper
