# -*- coding: utf-8 -*-

import os
import sys
import pytest
from importlib import import_module


# Insert current directory to the top in os.path for relative paths.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import django
django.setup()

from django.test.client import Client
from django.conf import settings
from django.apps import apps
from django.contrib.auth.models import AnonymousUser
from django.contrib.auth import login
try:
    from django.contrib.auth import get_user_model
except ImportError:
    from django.contrib.auth.models import User
    get_user_model = lambda: User


pytestmark = pytest.mark.django_db


def django_app(name):
    def wrapper():
        if name not in sys.modules:
            import_module(name)
        res = sys.modules[name]
        return res
    return wrapper


def django_model(model):
    def wrapper():
        return model
    return wrapper


for app_name in set(settings.INSTALLED_APPS):
    pytest.fixture(scope='session')(django_app(app_name))
for model in apps.get_models():
    pytest.fixture(scope='session')(django_model(model))


@pytest.fixture
def client():
    """A Django test client instance"""
    return Client()


@pytest.fixture
def anonymous_user():
    """ AnonymousUser instance"""
    return AnonymousUser()


@pytest.fixture
def test_user():
    """User instance"""
    kls = get_user_model()
    try:
        user = kls.objects.get(username='test')
    except kls.DoesNotExist:
        user = kls.objects.create(
            username='test',
            email='test@example.com',
            password='test',
            first_name='John',
            last_name='Doe',
            )
    return user


@pytest.fixture
def staff_user():
    """Staff User instance"""
    kls = get_user_model()
    try:
        staff = kls.objects.get(username='staff')
    except kls.DoesNotExist:
        staff = kls.objects.create(
            username='staff',
            email='staff@example.com',
            password='staff',
            first_name='Alice',
            last_name='Grace',
            is_staff=True
            )
    return staff


@pytest.fixture
def admin_user():
    """SuperUser instance"""
    kls = get_user_model()
    try:
        admin = kls.objects.get(username='admin')
    except kls.DoesNotExist:
        admin = kls.objects.create_superuser(
            username='admin',
            email='admin@example.com',
            password='admin',
            is_superuser=True
            )
    return admin


@pytest.fixture
def uclient(client, test_user, rf):
    """Client instance with logged in user"""
    test_user.backend = 'django.contrib.auth.backends.ModelBackend'
    if client.session:
        rf.session = client.session
    else:
        engine = import_module(settings.SESSION_ENGINE)
        rf.session = engine.SessionStore()
    login(rf, test_user)

    # Save the session values.
    rf.session.save()

    # Set the cookie to represent the session.
    session_cookie = settings.SESSION_COOKIE_NAME
    client.cookies[session_cookie] = rf.session.session_key
    cookie_data = {
        'max-age': None,
        'path': '/',
        'domain': settings.SESSION_COOKIE_DOMAIN,
        'secure': settings.SESSION_COOKIE_SECURE or None,
        'expires': None
        }
    client.cookies[session_cookie].update(cookie_data)
    client.user = test_user
    return client


@pytest.fixture
def aclient(client, admin_user, rf):
    """Client instance with logged in admin"""
    return uclient(client, admin_user, rf)
