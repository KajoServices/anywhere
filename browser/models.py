from django.db import models
from django.utils import timezone
from django.core.validators import MaxValueValidator, MinValueValidator
from django.shortcuts import render
from django.template import loader


LANGS = (
    ('en', 'English'),
    ('fr', 'Français'),
    ('es', 'Español'),
    ('de', 'Deutsch')
    )
COUNTRIES = ( # XXX TODO get the list of countries (`countries` app?)
    ('us', 'United States'),
    ('pl', 'Poland'),
    ('sk', 'Slovakia'),
    ('it', 'Italy'),
    ('ua', 'Ukraine'),
    ('ru', 'Russia'),
    ('kz', 'Kazakhstan')
    )
PROB_THRESHOLD = {'lo': 0., 'hi': 1.}
