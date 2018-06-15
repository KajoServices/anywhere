"""
Base settings for Anywhere project.
"""

import sys
from os import path


PROJECT_TITLE = "Anywhere"
BASE_DIR = path.dirname(path.dirname(path.abspath(__file__)))


def rel(*x):
    return path.join(BASE_DIR, *x)


# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.gis',
    'django_extensions',

    'wagtail.contrib.redirects',
    'wagtail.contrib.styleguide',
    'wagtail.contrib.forms',
    'wagtail.embeds',
    'wagtail.sites',
    'wagtail.users',
    'wagtail.snippets',
    'wagtail.documents',
    'wagtail.images',
    'wagtail.search',
    'wagtail.admin',
    'wagtail.core',

    'modelcluster',
    'taggit',

    'tastypie',
    'corsheaders',

    'core',
    'analytics',
    'dataman',
    'api',
    'browser',
]

MIDDLEWARE = [
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'corsheaders.middleware.CorsMiddleware',

    'wagtail.core.middleware.SiteMiddleware',
    'wagtail.contrib.redirects.middleware.RedirectMiddleware',
]

ROOT_URLCONF = 'urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            rel('templates'),
            rel('home', 'templates', 'home'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'wsgi.application'


# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': rel('db.sqlite3'),
    }
}


# Password validation
# https://docs.djangoproject.com/en/2.0/ref/settings/#auth-password-validators
AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.0/howto/static-files/
STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
]

STATIC_ROOT = rel('static')
STATICFILES_DIRS = [
    rel('staticfiles')
    ]
STATIC_URL = '/static/'

MEDIA_ROOT = rel('media')
MEDIA_URL = '/media/'


# Wagtail settings

WAGTAIL_SITE_NAME = "anywhere"

# Base URL to use when referring to full URLs within the Wagtail admin backend -
# e.g. in notification emails. Don't include '/admin' or a trailing slash
BASE_URL = 'http://anywhere.com'


# Django logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'request': {
            'format': "[%(asctime)s] %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S %z",
        },
        'verbose': {
            'format': "[%(asctime)s %(levelname)s] [%(module)s.%(name)s %(funcName)s:%(lineno)s] %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S %z",
        },
        'semiverbose': {
            'format': "[%(asctime)s %(levelname)s] [%(funcName)s] %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S %z",
        },
        'standard': {
            'format': "[%(asctime)s %(levelname)s] %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S %z",
        },
        'simple': {
            "format" : "[%(asctime)s %(levelname)s] %(message)s",
            'datefmt': "%H:%M:%S %z",
        }
    },
    'filters': {},
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
        'default': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': rel('log', 'app_root.log'),
            'maxBytes': 1024*1024*5, # 5 MB
            'backupCount': 1,
            'formatter': 'standard',
        },
        'request_handler': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': rel('log', 'app_django_access.log'),
            'maxBytes': 1024*1024*5, # 5 MB
            'backupCount': 1,
            'formatter': 'request',
        },
        'streaming': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': rel('log', 'app_stream.log'),
            'maxBytes': 1024*1024*10, # 10 MB
            'backupCount': 1,
            'formatter': 'semiverbose',
        },
        'tasks': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': rel('log',  'app_tasks.log'),
            'maxBytes': 1024*1024*10, # 10 MB
            'backupCount': 1,
            'formatter': 'semiverbose',
        }
    },
    'root': {
        'handlers': ['console'],
        'level': 'DEBUG'
    },
    'loggers': {
        'django.request': {
            'handlers': ['request_handler'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'commands': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'default': {
            'handlers': ['default'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'tasks': {
            'handlers': ['tasks'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'tweet': {
            'handlers': ['streaming'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}


# Elasticsearch
# number of document in a batch for scroll.
ES_INDEX = 'anywhere_v1'
ES_DOC_TYPE = 'tweet'
ES_TIMESTAMP_FIELD = 'created_at'
ES_SCROLL_BATCHSIZE = 5000
ES_MAX_RESULTS = 1000


# Hotspots on the map
HOTSPOT_MIN_ENTRIES = 2
HOTSPOTS_MAX_NUMBER = 20
HOTSPOTS_PRECISION = 5


# Celery
CELERY_ACCEPT_CONTENT = ['application/json', 'pickle']


# World borders file
WORLD_BORDERS = rel('countries', 'TM_WORLD_BORDERS-0.3.dbf')


# API settings
API_LIMIT_PER_PAGE = 36


# Load local settings
try:
    from .local import *
except Exception as err:
    print('Error loading local settings:\n%s\n\nUsing base settings.' % err)

