# -*- coding: utf-8 -*-
import pytest

import datetime
import dateparser
from mock import patch, Mock

from django.conf import settings
from django.utils import timezone

from core import utils


tz_systz = timezone.pytz.timezone(settings.TIME_ZONE)
tz_japan = timezone.pytz.timezone('Japan')
tz_lima = timezone.pytz.timezone('America/Lima')


@patch('core.utils.date', Mock(today=lambda: datetime.date(2017, 2, 3)))
def test_convert_timestamp_range():
    assert utils.date.today() == datetime.date(2017, 2, 3)

    assert utils.convert_time_range('today') == (
        datetime.datetime(2017, 2, 3, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2017, 2, 3, 23, 59, 59, tzinfo=tz_systz)
        )

    yesterday_japan = utils.convert_time_range('yesterday', tz_japan)
    assert utils.convert_time_range('yesterday', 'Japan') == yesterday_japan
    assert yesterday_japan == (
        datetime.datetime(2017, 2, 2, 0, 0, tzinfo=tz_japan),
        datetime.datetime(2017, 2, 2, 23, 59, 59, tzinfo=tz_japan)
        )

    assert utils.convert_time_range('this week') == (
        datetime.datetime(2017, 1, 30, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2017, 2, 5, 23, 59, 59, tzinfo=tz_systz)
        )
    assert utils.convert_time_range('last week') == (
        datetime.datetime(2017, 1, 23, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2017, 1, 29, 23, 59, 59, tzinfo=tz_systz),
        )

    this_month_lima = utils.convert_time_range('this month', tz_lima)
    assert utils.convert_time_range('this month', 'America/Lima') == this_month_lima
    assert this_month_lima == (
        datetime.datetime(2017, 2, 1, 0, 0, tzinfo=tz_lima),
        datetime.datetime(2017, 2, 28, 23, 59, 59, tzinfo=tz_lima)
        )

    assert utils.convert_time_range('this year') == (
        datetime.datetime(2017, 1, 1, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2017, 12, 31, 23, 59, 59, tzinfo=tz_systz)
        )
    assert utils.convert_time_range('last year') == (
        datetime.datetime(2016, 1, 1, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2016, 12, 31, 23, 59, 59, tzinfo=tz_systz)
        )


@patch('core.utils.date', Mock(today=lambda: datetime.date(2016, 2, 3)))
def test_convert_timestamp_range__leap_year():
    this_month_japan = utils.convert_time_range('this month', tz_japan)
    assert utils.convert_time_range('this month', 'Japan') == this_month_japan
    assert this_month_japan == (
        datetime.datetime(2016, 2, 1, 0, 0, tzinfo=tz_japan),
        datetime.datetime(2016, 2, 29, 23, 59, 59, tzinfo=tz_japan)
        )


def test_convert_timestamp_range__freeform():
    assert utils.convert_time_range('2015-05-08|2016-12-01') == (
        datetime.datetime(2015, 5, 8, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2016, 12, 1, 23, 59, 59, tzinfo=tz_systz)
        )
    assert utils.convert_time_range('2015-05-08|2015-05-09T12:15') == (
        datetime.datetime(2015, 5, 8, 0, 0, tzinfo=tz_systz),
        datetime.datetime(2015, 5, 9, 12, 15, tzinfo=tz_systz)
        )


def test_convert_timestamp_range__human_readable():
    def test_without_ms(start, end, test_start, test_end):
        start, end, test_start, test_end = [
            x.replace(microsecond=0) for x in (start, end, test_start, test_end)
            ]
        return (start, end) == (test_start, test_end)

    start, end = utils.convert_time_range('yesterday|now')
    test_start = timezone.now() + datetime.timedelta(days=-1)
    test_end = timezone.now()
    assert test_without_ms(start, end, test_start, test_end)

    start, end = utils.convert_time_range('2 hours ago|now')
    test_start = timezone.now() + datetime.timedelta(hours=-2)
    assert test_without_ms(start, end, test_start, test_end)


def test_convert_timestamp_range__exceptions():
    with pytest.raises(utils.MalformedValueError) as excinfo:
        assert utils.convert_time_range('2015-05-08|2015-05-07')
    assert str(excinfo.value) == 'Start date cannot be greater than the end date!'

    with pytest.raises(utils.MalformedValueError) as excinfo:
        assert utils.convert_time_range('rubbish')
    assert str(excinfo.value) == 'Cannot parse datetime range: wrong format!\nDatetime range should be two date[time] values divided by vertical bar (|)'

    with pytest.raises(utils.MalformedValueError) as excinfo:
        assert utils.convert_time_range('rubbish|rubbish')
    assert str(excinfo.value) == 'Cannot parse datetime range: wrong format!'

    with pytest.raises(utils.MalformedValueError) as excinfo:
        assert utils.convert_time_range('2 hours ago')
        assert str(excinfo.value) == 'Cannot parse datetime range: wrong format!'
