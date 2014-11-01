# -*- coding: utf-8 -*-
from django.db import models
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic


class DirtyInstance(models.Model):
    """
    Holds a reference to a model instance that may contain inconsistent data
    that needs to be recalculated.
    DirtyInstance instances are created by the insert/update/delete triggers
    when related objects change.
    """
    content_type = models.ForeignKey(ContentType)
    object_id = models.IntegerField(null=True, blank=True)
    content_object = generic.GenericForeignKey(fk_field="object_id")
    denormalizing_id = models.IntegerField(null=True, blank=True)
    denormalizing_lock_at = models.DateTimeField(null=True, blank=True)

    def __unicode__(self):
        return u'DirtyInstance: %s,%s' % (self.content_type, self.object_id)

    class Meta:
        unique_together = (
            ('content_type', 'object_id'),
        )
