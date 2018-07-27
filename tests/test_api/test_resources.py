# -*- coding: utf-8 -*-
import json
import pytest

from fixtures import *


pytestmark = pytest.mark.django_db


def test_tweets__all(tweets, test_user, client):
    objects = get_objects(client, API_TWEETS, get_params(test_user))
    assert len(objects) == 10


def test_tweets__get_list_default_ordering(tweets, test_user, client):
    objects = get_objects(client, API_TWEETS, get_params(test_user))
    assert [x["properties"]["created_at"] for x in objects] == [
        "2018-06-24T11:14:11",
        "2018-06-24T10:14:25",
        "2018-06-23T23:32:36",
        "2018-06-23T20:04:53",
        "2018-06-23T19:19:51",
        "2018-06-23T18:33:05",
        "2018-06-23T14:53:11",
        "2018-06-23T13:31:38",
        "2018-06-23T08:14:16",
        "2018-06-23T05:17:21"
        ]


def test_tweets__get_list_order_by(tweets, test_user, client):
    params = get_params(test_user)
    params.update(order_by="flood_probability")
    objects = get_objects(client, API_TWEETS, params)
    flood_prob = [0.418, 0.505, 0.801, 0.865, 0.876, 0.881, 0.945, 0.988, 0.991, 0.999]
    assert [x["properties"]["flood_probability"] for x in objects] == flood_prob

    params.update(order_by="-flood_probability")
    objects = get_objects(client, API_TWEETS, params)
    assert [x["properties"]["flood_probability"] for x in objects] == sorted(flood_prob, reverse=True)


def test_tweets__search_default_sorting_by_relevance(tweets, test_user, client):
    params = get_params(test_user)
    params.update(search="suitable living")
    objects = get_objects(client, API_TWEETS, params)
    score = [x["properties"]["score"] for x in objects]
    assert len(objects) == 3
    assert score == sorted(score, reverse=True)

    # &order_by= is ignored when &search= is present
    params.update(order_by='-created_at')
    objects = get_objects(client, API_TWEETS, params)
    score = [x["properties"]["score"] for x in objects]
    assert score == sorted(score, reverse=True)


def test_tweets__search(tweets, test_user, client):
    params = get_params(test_user)

    params.update(search="texas home")
    objects = get_objects(client, API_TWEETS, params)
    result = [13, 15, 14, 19]
    assert all(int(x["properties"]["id"]) in result for x in objects)

    params.update({"representative": "true"})
    objects = get_objects(client, API_TWEETS, params)
    assert [int(x["properties"]["id"]) for x in objects] == [13, 14]


def test_tweets__get_list_timestamp_filter(tweets, test_user, client):
    params = get_params(test_user)
    params.update({"created_at": "2018-06-24|now"})
    objects = get_objects(client, API_TWEETS, params)
    assert len(objects) == 2


def test_tweets__get_list_timestamp_filter_bad_request(tweets, test_user, client):
    params = get_params(test_user)
    params.update({"created_at": "now"})
    resp = client.get(API_TWEETS, params)
    assert resp.status_code == 400


def test_tweets__agg_timestamp(tweets, test_user, client):
    params = get_params(test_user)
    params.update({"agg_timestamp": 1})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    agg_total_count = sum(x["doc_count"] for x in content["aggregations"]["agg_timestamp"])
    assert len(content["aggregations"]["agg_timestamp"]) == 181
    assert agg_total_count == content["meta"]["total_count"]

    params.update({"agg_timestamp__interval": "1h"})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert len(content["aggregations"]["agg_timestamp"]) == 31
    assert sum(x["doc_count"] for x in content["aggregations"]["agg_timestamp"]) == agg_total_count

    params.update({"created_at": "2018-06-24|now"})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert content["aggregations"]["agg_timestamp"][0]["key_as_string"] == "2018-06-24T10:00:00.000Z"
    assert content["aggregations"]["agg_timestamp"][1]["key_as_string"] == "2018-06-24T11:00:00.000Z"
    assert len(content["aggregations"]["agg_timestamp"]) == 2
    assert sum(x["doc_count"] for x in content["aggregations"]["agg_timestamp"]) == \
        content["meta"]["total_count"]

    # Switch off features entirely.
    params.update({"size": 0})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert "aggregations" in content
    assert content["features"] == []

    del params["agg_timestamp"]
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert "aggregations" not in content


def test_tweets__agg_floodprob(tweets, test_user, client):
    params = get_params(test_user)
    params.update({"agg_floodprob": 1})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    agg_total_count = sum(x["doc_count"] for x in content["aggregations"]["agg_floodprob"])
    assert len(content["aggregations"]["agg_floodprob"]) == 181
    assert agg_total_count == content["meta"]["total_count"]
    assert all(x["avg_flood_probability"] is not None for x in content["aggregations"]["agg_floodprob"]
        if x["doc_count"] > 0)

    params.update({"agg_floodprob__interval": "12h"})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert len(content["aggregations"]["agg_floodprob"]) == 3
    assert sum(x["doc_count"] for x in content["aggregations"]["agg_floodprob"]) == agg_total_count

    params.update({"created_at__lt": "2018-06-24"})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert len(content["aggregations"]["agg_floodprob"]) == 2


@pytest.xfail
def test_tweets__agg_hotspot(tweets, test_user, client):
    params = get_params(test_user)
    params.update({"agg_hotspot": 1})
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    agg_total_count = sum(x["doc_count"] for x in content["aggregations"]["agg_hotspot"])
    assert len(content["aggregations"]["agg_hotspot"]) == 5
    assert agg_total_count == content["meta"]["total_count"]

    params.update(search="suitable living")
    resp = client.get(API_TWEETS, params)
    content = json.loads(resp.content.decode('utf-8'))
    assert len(content["aggregations"]["agg_hotspot"]) == 1

# TODO
# - /tweet/ POST, DELETE and PATCH
# - other endpoints
