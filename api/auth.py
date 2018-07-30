from django.utils.translation import ugettext_lazy as _
from django.contrib.sessions.models import Session
from django.contrib.auth.models import Group, Permission, User
from django.contrib.auth import get_user_model

from tastypie.authentication import BasicAuthentication, ApiKeyAuthentication
from tastypie.authorization import Authorization
from tastypie.exceptions import Unauthorized


MSG_UNAUTHORIZED = "You cannot do this, sorry. Only admins are allowed to perform this operation."


class CookieBasicAuthentication(BasicAuthentication):
    """
    If the user is already authenticated by a django session it will
    allow the request (useful for ajax calls). If it is not, defaults
    to basic authentication, which other clients could use.
    """

    def __init__(self, *args, **kwargs):
        self.super_self = super(CookieBasicAuthentication, self)
        self.super_self.__init__(*args, **kwargs)

    def is_authenticated(self, request, **kwargs):
        if 'sessionid' in request.COOKIES:
            s = Session.objects.get(pk=request.COOKIES['sessionid'])
            if '_auth_user_id' in s.get_decoded():
                request.user = User.objects.get(
                    id=s.get_decoded()['_auth_user_id']
                )
                return True
        return self.super_self.is_authenticated(request, **kwargs)


class UserAuthorization(Authorization):
    """
    Authorization of ordinary users.
    """
    def authorized(self, object_list, bundle):
        """Checks if a user is superuser of staff member."""
        user = bundle.request.user
        try:
            return user.is_active
        except AttributeError:
            raise Unauthorized(_('You have to authenticate first!'))

    def authorized_list_auth(self, object_list, bundle):
        """
        Returns object_list for superusers or staff members,
        otherwise returns empty list.
        """
        if self.authorized(object_list, bundle):
            return object_list
        return []

    def read_list(self, object_list, bundle):
        """
        Returns data from object_list for superusers or staff members.
        This assumes a QuerySet from ModelResource, therefore tries to return
        .all(), or original object_list in case of failure.
        """
        try:
            object_list = object_list.all()
        except AttributeError:
            # list or dict don't have .all()
            pass
        return self.authorized_list_auth(object_list, bundle)

    def read_detail(self, object_list, bundle):
        if bundle.request.user.is_anonymous:
            # Double-check anonymous users, because operations
            # on embedded fields do not pass through authentication.
            ApiKeyAuthentication().is_authenticated(bundle.request)
        return self.authorized(object_list, bundle)

    def create_list(self, object_list, bundle):
        raise Unauthorized(_(MSG_UNAUTHORIZED))

    def su_or_staff(self, bundle):
        user = bundle.request.user
        try:
            return user.is_active and (user.is_superuser or user.is_staff)
        except AttributeError:
            raise Unauthorized(_('You have to authenticate first!'))

    def create_detail(self, object_list, bundle):
        """Superusers and staff can create docs."""
        return self.su_or_staff(bundle)

    def update_list(self, object_list, bundle):
        raise Unauthorized(_(MSG_UNAUTHORIZED))

    def update_detail(self, object_list, bundle):
        raise Unauthorized(_(MSG_UNAUTHORIZED))

    def delete_list(self, object_list, bundle):
        raise Unauthorized(_(MSG_UNAUTHORIZED))

    def delete_detail(self, object_list, bundle):
        """Superusers and staff can delete docs."""
        return self.su_or_staff(bundle)


class StaffAuthorization(Authorization):
    """
    Class for staff authorization.
    """
    def authorized(self, object_list, bundle):
        """Checks if a user is superuser of staff member."""
        user = bundle.request.user
        try:
            return user.is_active and (user.is_superuser or user.is_staff)
        except AttributeError:
            raise Unauthorized(_('You have to authenticate first!'))

    def authorized_list_auth(self, object_list, bundle):
        """
        Returns object_list for superusers or staff members,
        otherwise returns empty list.
        """
        if self.authorized(object_list, bundle):
            return object_list
        return []

    def read_list(self, object_list, bundle):
        """
        Returns data from object_list for superusers or staff members.
        This assumes a QuerySet from ModelResource, therefore tries to return
        .all(), or original object_list in case of failure.
        """
        try:
            object_list = object_list.all()
        except AttributeError:
            # dict doesn't have .all()
            pass
        return self.authorized_list_auth(object_list, bundle)

    def read_detail(self, object_list, bundle):
        if bundle.request.user.is_anonymous:
            # Double-check anonymous users, because operations
            # on embedded fields do not pass through authentication.
            ApiKeyAuthentication().is_authenticated(bundle.request)
        return self.authorized(object_list, bundle)

    def create_list(self, object_list, bundle):
        return self.authorized_list_auth(object_list, bundle)

    def create_detail(self, object_list, bundle):
        return self.authorized(object_list, bundle)

    def update_list(self, object_list, bundle):
        return self.authorized_list_auth(object_list, bundle)

    def update_detail(self, object_list, bundle):
        return self.authorized(object_list, bundle)

    def delete_list(self, object_list, bundle):
        """Superuser and staff can delete lists."""
        return self.authorized_list_auth(object_list, bundle)

    def delete_detail(self, object_list, bundle):
        """Superuser and staff can delete item."""
        return self.authorized(object_list, bundle)


class OwnerAuthorization(Authorization):
    """
    Custom authorization using following permissions:
        - Lists and individual entries can be viewed by active superusers,
          and entry owners. If a entry doesn't have a field 'owner',
          the whole list is being returned.
        - When a record is created, current user authomatically becomes it's
          owner (that's why it isn't necessary to fill 'owner' field).
    """

    def authorized(self, user):
        return user.is_superuser and user.is_active

    def owned_objects(self, object_list, bundle):
        try:
            return [x for x in object_list if x.user == bundle.request.user]
        except AttributeError:
            if isinstance(object_list[0], get_user_model()):
                return [x for x in object_list if x == bundle.request.user]
        return []

    def check_list(self, object_list, bundle):
        user = bundle.request.user
        if user.is_anonymous:
            raise Unauthorized(_('You have to authenticate first!'))

        if (user.is_superuser or user.is_staff) and user.is_active:
            try:
                return object_list.all()
            except AttributeError:  # dict doesn't have .all()
                return object_list
        else:
            return self.owned_objects(object_list, bundle)

    def read_list(self, object_list, bundle):
        return self.check_list(object_list, bundle)

    def read_detail(self, object_list, bundle):
        if self.check_list(object_list, bundle):
            return True
        return False

    def create_detail(self, object_list, bundle):
        if bundle.request.user.is_anonymous:
            return False
        try:
            bundle.obj.owner = bundle.request.user
        except AttributeError:
            pass
        return True

    def create_list(self, object_list, bundle):
        return self.check_list(object_list, bundle)

    def update_detail(self, object_list, bundle):
        return not bundle.request.user.is_anonymous

    def update_list(self, object_list, bundle):
        allowed = []
        # Since they may not all be saved, iterate over them.
        for obj in object_list:
            if self.update_detail([obj], bundle):
                allowed.append(obj)
        return allowed

    def delete_list(self, object_list, bundle):
        """
        Only superuser can delete lists!
        """
        if self.bundle.request.user.is_superuser:
            return True
        raise Unauthorized("You cannot delete a list.")

    def delete_detail(self, object_list, bundle):
        """
        If user has permissions to delete detail, it can only
        delete those ctreated by him.
        """
        return bundle.obj.owner == bundle.request.user


class AnonymousCanPostAuthorization(OwnerAuthorization):
    """
    Class for staff authorization, in which detail can be created
    by anonymous user.
    """
    def create_detail(self, object_list, bundle):
        """
        SU or staff can decide to create a user,
        anonymous user mean registration.
        """
        return self.authorized(bundle.request.user) \
               or bundle.request.user.is_anonymous

    def create_list(self, object_list, bundle):
        """
        Create list is the same as create_detail, since the URI
        ends with /<resource_name>/
        """
        return self.create_detail(object_list, bundle)


class RegisteredCanViewAuthorization(OwnerAuthorization):
    """
    Custom authorization:
    - lists and details can be viewed by registered users
    - any update requires superuser status
    """
    def check_list(self, object_list, bundle):
        if bundle.request.user.is_anonymous:
            raise Unauthorized(_('You have to authenticate first!'))
        return object_list

    def create_detail(self, object_list, bundle):
        return self.authorized(bundle.request.user)

    def create_list(self, object_list, bundle):
        if self.authorized(bundle.request.user):
            return self.check_list(object_list, bundle)
        return []

    def update_detail(self, object_list, bundle):
        return self.authorized(bundle.request.user)

    def delete_list(self, object_list, bundle):
        return self.authorized(bundle.request.user)

    def delete_detail(self, object_list, bundle):
        return self.authorized(bundle.request.user)
