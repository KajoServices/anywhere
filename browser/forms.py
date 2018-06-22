# -*- coding: utf-8 -*-
import datetime

from django import forms
from django.utils import timezone
from django.contrib.admin import widgets

from .models import LANGS, COUNTRIES, PROB_THRESHOLD


DATE_FORMAT = 'YYYY-MM-DD'
TIME_FORMAT = 'HH:mm:ss'


class FloodMapFiltersForm(forms.Form):
    date_from = forms.DateField(
        initial=timezone.datetime.today(),
        widget=forms.SelectDateWidget,
        help_text='Start date ({})'.format(DATE_FORMAT)
        )
    time_from = forms.TimeField(
        initial=timezone.now()-datetime.timedelta(hours=1),
        widget=forms.TimeInput(format='%H:%M'),
        help_text='Start time ({})'.format(TIME_FORMAT)
        )
    date_to = forms.DateField(
        initial=timezone.datetime.today(),
        widget=forms.SelectDateWidget,
        help_text='End date ({})'.format(DATE_FORMAT)
        )
    time_to = forms.TimeField(
        initial=timezone.now(),
        widget=forms.TimeInput(format='%H:%M'),
        help_text='End time ({})'.format(TIME_FORMAT)
        )
    lang = forms.ChoiceField(choices=LANGS, help_text='Choose language')
    country = forms.ChoiceField(choices=COUNTRIES, help_text='Choose country')
    flood_probability = forms.FloatField(
        min_value=PROB_THRESHOLD['lo'],
        max_value=PROB_THRESHOLD['hi'],
        initial=0.6,
        widget=forms.NumberInput(attrs={'step': "0.01"}),
        help_text='Threshold for probability of the flood (min: {lo}, max: {hi})'\
                  .format(**PROB_THRESHOLD),
        localize=False
        )
