# -*- coding: utf-8 -*-
from django.shortcuts import render
from django.views.generic import FormView
from django import forms

from .forms import FloodMapFiltersForm
from .mixins import AjaxFormMixin
from .models import PROB_THRESHOLD


class FloodMapView(AjaxFormMixin, FormView):
    form_class = FloodMapFiltersForm
    template_name  = 'browser/flood_map_page.html'

    def clean_flood_prob_threshold(self):
        threshold = self.cleaned_data.get("clean_flood_prob_threshold")
        if threshold < PROB_THRESHOLD['lo'] or threshold > PROB_THRESHOLD['hi']:
            raise forms.ValidationError(
                "Flood probalility threshold should be between {lo} and {hi}".format(
                    **PROB_THRESHOLD
                    )
                )
        return threshold

    def post(request):
        form = FloodMapFiltersForm(request.POST)
        if form.is_valid():
            return render(request, self.template_name, {
                'form': form,
                })

    def get(request):
        form = FloodMapFiltersForm()
        return render(request, self.template_name, {'form': form})
