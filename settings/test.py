from .base import *

DEBUG = False
TEST_RUN = True

ALLOWED_HOSTS = ['127.0.0.1', 'localhost', 'testserver']

# Haystack
HAYSTACK_SIGNAL_PROCESSOR = 'haystack.signals.BaseSignalProcessor'
HAYSTACK_CONNECTIONS = {
    'default': {
        'ENGINE': 'haystack.backends.elasticsearch_backend.ElasticsearchSearchEngine',
        'URL': 'http://127.0.0.1:9200/',
        'INDEX_NAME': 'test',
    },
}

# Elasticsearch
# number of document in a batch for scroll.
ES_INDEX = ES_INDEX + '_test'
ES_SCROLL_BATCHSIZE = 10
ES_MAX_RESULTS = 10

# Print emails to the console.
EMAIL_BACKEND = 'django.core.mail.backends.console.EmailBackend'

# celery
BROKER_URL = "redis://"
CELERY_RESULT_BACKEND = BROKER_URL
CELERY_ALWAYS_EAGER = True
