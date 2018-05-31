from django.db import models
from django.db.models.signals import post_save
from django.contrib.auth.models import User
from django.dispatch import receiver
from tastypie import models as tastypie_models

from .utils import deep_update


class DictDocumentMixin(object):
    """
    Mixin providing self._dict property constructed using self._fields,
    updated by self.__dict__ (or simply  self.__dict__,
    if self._fields isn't available).
    """
    @property
    def _dict(self):
        try:
            _dict = dict((f, getattr(self, f)) for f in self._fields)
        except (KeyError, AttributeError):
            return self.__dict__

        return deep_update(_dict, self.__dict__)


class UserProfile(models.Model, DictDocumentMixin):
    user = models.OneToOneField(
        User, related_name='profile', on_delete=models.CASCADE
        )
    email_confirmed = models.BooleanField(default=False)


def ensure_api_key(user):
    """
    Auto-create API key when user is saved.
    """
    try:
        user.api_key
    except tastypie_models.ApiKey.DoesNotExist:
        tastypie_models.ApiKey.objects.create(user=user)


def update_user_profile(user, created):
    if created:
        UserProfile.objects.create(user=user)
        return

    try:
        user.profile.save()
    except UserProfile.DoesNotExist:
        UserProfile.objects.create(user=user)


@receiver(post_save, sender=User)
def user_post_save(sender, instance, created, **kwargs):
    update_user_profile(instance, created)
    ensure_api_key(instance)
