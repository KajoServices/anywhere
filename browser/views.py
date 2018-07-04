# -*- coding: utf-8 -*-
from django.shortcuts import render
from django.views.generic import FormView
from django import forms

from .forms import FloodMapFiltersForm
from .mixins import AjaxFormMixin
from .models import PROB_THRESHOLD


def get_floodmap(request):
    template_name = 'browser/flood_map_page.html'
    data = {'form_url': '/floodmap/'}
    if request.method == 'POST':
        form = FloodMapFiltersForm(request.POST)
        if form.is_valid():
            return render(request, template_name, {'form': form, 'data': data})
    else:
        form = FloodMapFiltersForm(request.POST)
    return render(request, template_name, {'form': form, 'data': data})
