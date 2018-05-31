from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.models import User


def admin_method_attrs(**outer_kwargs):
    """
    Wrap an admin method with passed arguments as attributes and values.
    (common admin manipulation such as setting  short_description, etc.)
    """
    def method_decorator(func):
        for kw, arg in outer_kwargs.items():
            setattr(func, kw, arg)
        return func
    return method_decorator


class AppUserAdmin(UserAdmin):
    def __init__(self, *args, **kwargs):
        super(AppUserAdmin, self).__init__(*args, **kwargs)
        AppUserAdmin.list_display = (
            'username', '_is_staff', '_is_superuser',
            '_is_active', '_email_confirmed',
            '_full_name', 'date_joined', 'last_login',
            )
        AppUserAdmin.list_filter = (
            'date_joined', 'last_login',
            'is_staff', 'is_superuser', 'is_active',
            'profile__email_confirmed',
            )

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return self.readonly_fields + ('date_joined', 'last_login',)
        return self.readonly_fields

    @admin_method_attrs(short_description='active',
                        admin_order_field='is_active',
                        boolean=True)
    def _is_active(self, obj):
        return obj.is_active

    @admin_method_attrs(short_description='staff',
                        admin_order_field='is_staff',
                        boolean=True)
    def _is_staff(self, obj):
        return obj.is_staff

    @admin_method_attrs(short_description='admin',
                        admin_order_field='is_superuser',
                        boolean=True)
    def _is_superuser(self, obj):
        return obj.is_superuser

    @admin_method_attrs(short_description='email conf', boolean=True)
    def _email_confirmed(self, obj):
        return obj.profile.email_confirmed

    @admin_method_attrs(short_description='full name')
    def _full_name(self, obj):
        return obj.get_full_name()


admin.site.unregister(User)
admin.site.register(User, AppUserAdmin)
