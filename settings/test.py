from .base import *

DEBUG = False
TEST_RUN = True

ALLOWED_HOSTS = ['127.0.0.1', 'localhost', 'testserver']

# Elasticsearch
# number of document in a batch for scroll.
ES_INDEX = ES_INDEX + '_test'
ES_SCROLL_BATCHSIZE = 10
ES_MAX_RESULTS = 10
ES_PORT = 9200
ES_CLIENT = Elasticsearch(
    [ES_ADDRESS],
    port=ES_PORT,
    timeout=30,
    max_retries=10,
    retry_on_timeout=True
    )

# Print emails to the console.
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

# celery
BROKER_URL = "redis://"
CELERY_RESULT_BACKEND = BROKER_URL
CELERY_ALWAYS_EAGER = True

HOTSPOTS_PRECISION = 1
HOTSPOT_MIN_ENTRIES = 1
