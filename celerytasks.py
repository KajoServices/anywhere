import os
import sys
import time
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import json
import datetime
from celery import Celery
from celery.task.base import periodic_task
from celery.task.schedules import crontab

import settings.base as conf
from dataman import cassandra, elastic, normalizer
from core.utils import timeit


app = Celery('celerytasks')
app.conf.broker_url = conf.BROKER_URL
app.conf.result_backend = conf.RESULT_BACKEND
app.conf.accept_content = conf.CELERY_ACCEPT_CONTENT

INDEX_UPDATE_TIME_LIMIT = conf.CASSANDRA_BEAT + 60


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
    norm = normalizer.TweetNormalizer(doc)
    obj = norm.normalize()
    try:
        res = elastic.create_or_update_index(_id, obj)
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


@periodic_task(run_every=datetime.timedelta(seconds=conf.CASSANDRA_BEAT))
def monitor_new_records():
    """
    Periodically start collecting new records for the last N hours.
    """
    timestamp = datetime.datetime.utcnow() \
              - datetime.timedelta(hours=conf.CASSANDRA_TIMEDELTA)
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
    batch_size = conf.ES_SCROLL_BATCHSIZE
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
