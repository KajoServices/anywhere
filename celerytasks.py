import os
import sys
import time
import json
import datetime

from django.conf import settings
from django.utils import timezone
from django.db.models.constants import LOOKUP_SEP

from celery import Celery
from celery.task.base import periodic_task
from celery.task.schedules import crontab

from dataman import cassandra, elastic
from dataman.processors import categorize_repr_docs, TweetNormalizer, \
     ClusterBuilder, GeoClusterBuilder


app = Celery('celerytasks')
app.conf.broker_url = settings.BROKER_URL
app.conf.result_backend = settings.RESULT_BACKEND
app.conf.accept_content = settings.CELERY_ACCEPT_CONTENT

INDEX_UPDATE_TIME_LIMIT = settings.CASSANDRA_BEAT + 60


def update_doc(doc):
    """
    Inject keys necessary for ES (mainly elastic-specific data).
    """
    # Fill tokens.
    try:
        text = doc['text']
    except KeyError:
        tokens = []
    else:
        try:
            lang = doc['lang']
        except KeyError:
            lang = None
        tokens = elastic.tokenize(text, lang=lang)
    doc.update({'tokens': tokens})
    return doc


def process_doc(_id, doc):
    norm = TweetNormalizer(doc)
    obj = norm.normalize()
    try:
        res = elastic.create_or_update_doc(_id, obj)
    except Exception as err:
        print("! [process_doc] Could not add doc {} to index:\n  {}".format(
            doc['tweetid'], err))
        res = 'failed'
    # If sucessful `res` is either 'created' or 'updated'
    return res


def es_index_update(timestamp, timestamp_to=None):
    elastic.ensure_mapping()
    cass = cassandra.CassandraProxy()
    result = {'created': 0, 'updated': 0, 'failed': 0}
    for doc in cass.get_data(timestamp, timestamp_to=timestamp_to):
        res = process_doc(doc['tweetid'], doc)
        result[res] += 1
    return result


@app.task(time_limit=INDEX_UPDATE_TIME_LIMIT,
          soft_time_limit=INDEX_UPDATE_TIME_LIMIT)
def run_index_update(timestamp):
    report = {}
    msg = ''
    try:
        result = es_index_update(timestamp)
        msg = ". [run_index_update] Done: {}".format(result)
    except Exception as err:
        report.update(success=False, error=err)
        msg = "! [run_index_update] Failed: {}".format(err)
    else:
        report.update(success=True)
        report.update(result)
    finally:
        print(msg)

    return report


@periodic_task(run_every=datetime.timedelta(seconds=settings.CASSANDRA_BEAT))
def monitor_new_records():
    """
    Periodically start collecting new records for the last N hours.
    """
    timestamp = datetime.datetime.utcnow() \
              - datetime.timedelta(hours=settings.CASSANDRA_TIMEDELTA)
    run_index_update.delay(timestamp)


@app.task
def process_batch(batch):
    results = {'created': 0, 'updated': 0, 'failed': 0}
    for rec in batch:
        result = process_doc(rec['_id'], rec['_source'])
        results[result] += 1
    print("..[process_batch] Processed {}".format(results))
    return results


@periodic_task(run_every=crontab(hour=23))
def full_reindex():
    total = elastic.return_all()['hits']['total']
    batch_size = settings.ES_SCROLL_BATCHSIZE
    processed = 0
    scroll_id = None

    while processed < total:
        if scroll_id is None:
            query = {"query": {"match_all": {}}, 'size': batch_size}
            response = elastic.search(query, scroll=True)
        else:
            response = elastic.scroll(scroll_id)

        scroll_id = response['_scroll_id']
        process_batch.delay(response['hits']['hits'])
        processed += batch_size

    print(". [full_reindex] finished")


def delete_retweets(*terms, **filters):
    if settings.ES_GEO_FIELD in terms:
        # Clustering tweets by geolocation.
        terms = tuple(x for x in terms if x != settings.ES_GEO_FIELD)
        cb = GeoClusterBuilder(*terms, **filters)
    else:
        cb = ClusterBuilder(*terms, **filters)
    result = cb.get_clusters()

    # Select representative tweets for each cluster.
    for cluster in result["clusters"]:
        categorized = categorize_repr_docs(cluster["docs"])
        for doc in categorized["non_representative_docs"]:
            elastic.delete_doc(doc["_id"])


@periodic_task(run_every=crontab(minute=settings.SEGMENTER_TIMEFRAME))
def task_retain_representative_tweets():
    timestamp_gte = settings.ES_TIMESTAMP_FIELD + '__gte'
    past = (timezone.now() - timezone.timedelta(minutes=settings.SEGMENTER_TIMEFRAME))
    filters = {timestamp_gte: past.isoformat()}
    delete_retweets(filters)
