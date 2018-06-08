from django.db import models
from django.utils import timezone
from django.core.validators import MaxValueValidator, MinValueValidator

from wagtail.core.models import Page
from wagtail.core.fields import StreamField
from wagtail.core import blocks
from wagtail.admin.edit_handlers import FieldPanel, StreamFieldPanel, FieldRowPanel
from wagtail.core import blocks

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

class HomePage(Page):
    pass


class HistogramBlock(blocks.StructBlock):
    tweets_per_time = blocks.RawHTMLBlock() # XXX - or EmbedBlock?
    prob_classes_time = blocks.RawHTMLBlock()

    class Meta:
        icon = 'view'
        form_classname = 'struct-block'


class GraphTopBlock(blocks.StructBlock):
    top_score = blocks.RawHTMLBlock()
    top_users = blocks.RawHTMLBlock()
    top_locations = blocks.RawHTMLBlock()
    top_tokens = blocks.RawHTMLBlock()

    class Meta:
        icon = 'view'
        form_classname = 'struct-block'


class HeatmapBlock(blocks.StructBlock):
    heading = blocks.CharBlock(classname="heading")
    heatmap = blocks.RawHTMLBlock()

    class Meta:
        icon = 'view'
        form_classname = 'struct-block'


class GraphBlock(blocks.StructBlock):
    top_graph = GraphTopBlock()
    heatmap = HeatmapBlock()

    class Meta:
        icon = 'view'
        form_classname = 'struct-block'


class TweetTableBlock(blocks.StructBlock):
    heading = blocks.CharBlock(classname="heading")
    table = blocks.RawHTMLBlock()

    class Meta:
        icon = 'tab'
        form_classname = 'struct-block'


class FloodMapPage(Page):
    time_range_text = models.CharField(max_length=255, blank=True, default='')
    date_from = models.DateField(blank=True, default=timezone.datetime.today)
    time_from = models.TimeField(blank=True, default=timezone.now)
    date_to = models.DateField(blank=True, default=timezone.datetime.today)
    time_to = models.TimeField(blank=True, default=timezone.now)
    lang = models.CharField(
        max_length=25,
        choices=LANGS,
        null=True,
        blank=True,
        help_text="languages"
        )
    country = models.CharField(
        max_length=255,
        choices=COUNTRIES,
        null=True,
        blank=True,
        help_text="countries"
        )
    flood_prob_threshold = models.FloatField(
        validators=[MinValueValidator(0.), MaxValueValidator(1.)],
        default=0.6,
        help_text="flood probability threshold"
        )
    histo = StreamField([
        ('content', HistogramBlock()),
        ])
    graph = StreamField([
        ('content', GraphBlock()),
        ])
    tweets = StreamField([
        ('content', TweetTableBlock()),
        ])

    content_panels = Page.content_panels + [
        FieldPanel('time_range_text'),
        FieldRowPanel([
            FieldPanel('date_from', classname="date_from"),
            FieldPanel('time_from', classname="time_from"),
            FieldPanel('date_to', classname="date_to"),
            FieldPanel('time_to', classname="time_to"),
            ]),
        FieldPanel('lang'),
        FieldPanel('country'),
        FieldPanel('flood_prob_threshold'),
        StreamFieldPanel('histo'),
        StreamFieldPanel('graph'),
        StreamFieldPanel('tweets'),
        ]
