from django.contrib import admin

from denorm.models import DirtyInstance


class DirtyInstanceAdmin(admin.ModelAdmin):
    list_display = (
        'content_type', 'object_id', 'denormalizing_id',
        'denormalizing_lock_at',
    )
    list_filter = ('denormalizing_id',)
    readonly_fields = (
        'content_type', 'object_id', 'denormalizing_id',
        'denormalizing_lock_at',
    )


admin.site.register(DirtyInstance, DirtyInstanceAdmin)
