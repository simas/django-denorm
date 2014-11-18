# -*- coding: utf-8 -*-
import abc
import datetime
import random

import logging
from django.contrib.contenttypes.models import ContentType
from denorm.db import triggers
from django.db import connection
from django.core.cache import cache
from django.db import transaction
from django.db.models import sql, ManyToManyField
from django.db.models.aggregates import Sum
from django.db.models.fields.related import ManyToManyField
from django.db.models.loading import get_model
from django.db.models.manager import Manager
from denorm.models import DirtyInstance

# remember all denormalizations.
# this is used to rebuild all denormalized values in the whole DB
from django.db.models.query_utils import Q
from django.db.models.sql import Query
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.constants import JoinInfo
from django.db.models.sql.query import Query
from django.db.models.sql.where import WhereNode

# Remember all denormalizations.
# This is used to rebuild all denormalized values in the whole DB.
alldenorms = []


log = logging.getLogger(__name__)


def many_to_many_pre_save(sender, instance, **kwargs):
    """
    Updates denormalised many-to-many fields for the model
    """
    if instance.pk:
        # Need a primary key to do m2m stuff
        for m2m in sender._meta.local_many_to_many:
            # This gets us all m2m fields, so limit it to just those that are denormed
            if hasattr(m2m, 'denorm'):
                # Does some extra jiggery-pokery for "through" m2m models.
                # May not work under lots of conditions.
                if hasattr(m2m.rel, 'through_model'):
                    # Clear exisiting through records (bit heavy handed?)
                    kwargs = {m2m.related.var_name: instance}

                    # Can't use m2m_column_name in a filter
                    # kwargs = { m2m.m2m_column_name(): instance.pk, }
                    m2m.rel.through_model.objects.filter(**kwargs).delete()

                    values = m2m.denorm.func(instance)
                    for value in values:
                        kwargs.update({m2m.m2m_reverse_name(): value.pk})
                        m2m.rel.through_model.objects.create(**kwargs)

                else:
                    values = m2m.denorm.func(instance)
                    setattr(instance, m2m.attname, values)


def many_to_many_post_save(sender, instance, created, **kwargs):
    if created:
        def check_resave():
            for m2m in sender._meta.local_many_to_many:
                if hasattr(m2m, 'denorm'):
                    return True
            return False

        if check_resave():
            instance.save()


class Denorm(object):
    def __init__(self, skip=None):
        self.func = None
        self.skip = skip

    def setup(self, **kwargs):
        """
        Adds 'self' to the global denorm list
        and connects all needed signals.
        """
        global alldenorms
        if self not in alldenorms:
            alldenorms.append(self)

    def update(self, qs):
        """
        Updates the denormalizations in all instances in the queryset 'qs'.
        """
        for instance in qs.distinct().iterator():
            # only write new values to the DB if they actually changed
            new_value = self.func(instance)

            # Get attribute name (required for denormalising ForeignKeys)
            attname = instance._meta.get_field(self.fieldname).attname

            if isinstance(getattr(instance, attname), Manager):
                # for a many to many field the decorated
                # function should return a list of either model instances
                # or primary keys
                old_pks = set([x.pk for x in getattr(instance, attname).all()])
                new_pks = set([])

                for x in new_value:
                    # we need to compare sets of objects based on pk values,
                    # as django lacks an identity map.
                    if hasattr(x,'pk'):
                        new_pks.add(x.pk)
                    else:
                        new_pks.add(x)

                if old_pks != new_pks:
                    print old_pks
                    for o in qs.filter(pk=instance.pk):
                        o.attname = new_value
                    instance.save()

            elif not getattr(instance, attname) == new_value:
                setattr(instance, attname, new_value)
                # an update before the save is needed to handle CountFields
                # CountField does not update its value during pre_save
                qs.filter(pk=instance.pk).update(**{self.fieldname: new_value})
                instance.save()
        flush()

    def get_triggers(self, using):
        return []


class BaseCallbackDenorm(Denorm):
    """
    Handles the denormalization of one field, using a python function
    as a callback.
    """

    def setup(self, **kwargs):
        """
        Calls setup() on all DenormDependency resolvers
        """
        super(BaseCallbackDenorm, self).setup(**kwargs)

        for dependency in self.depend:
            dependency.setup(self.model)

    def get_triggers(self, using):
        """
        Creates a list of all triggers needed to keep track of changes
        to fields this denorm depends on.
        """
        trigger_list = list()

        # Get the triggers of all DenormDependency instances attached
        # to our callback.
        for dependency in self.depend:
            trigger_list += dependency.get_triggers(using=using)

        return trigger_list + super(BaseCallbackDenorm, self).get_triggers(using=using)


class CallbackDenorm(BaseCallbackDenorm):
    """
    As above, but with extra triggers on self as described below
    """

    def get_triggers(self, using):
        content_type = str(ContentType.objects.get_for_model(self.model).pk)

        # Create a trigger that marks any updated or newly created
        # instance of the model containing the denormalized field
        # as dirty.
        # This is only really needed if the instance was changed without
        # using the ORM or if it was part of a bulk update.
        # In those cases the self_save_handler won't get called by the
        # pre_save signal, so we need to ensure flush() does this later.
        action = triggers.TriggerActionInsert(
            model=DirtyInstance,
            columns=("content_type_id", "object_id"),
            values=(content_type, "NEW.%s" % self.model._meta.pk.get_attname_column()[1])
        )
        trigger_list = [
            triggers.Trigger(self.model, "after", "update", [action], content_type, using, self.skip),
            triggers.Trigger(self.model, "after", "insert", [action], content_type, using, self.skip),
        ]

        return trigger_list + super(CallbackDenorm, self).get_triggers(using=using)


class BaseCacheKeyDenorm(Denorm):
    def __init__(self, depend_on_related, *args, **kwargs):
        self.depend = depend_on_related
        super(BaseCacheKeyDenorm, self).__init__(*args, **kwargs)
        self.func = lambda o: random.randint(-9223372036854775808, 9223372036854775807)

    def setup(self, **kwargs):
        """
        Calls setup() on all DenormDependency resolvers
        """
        super(BaseCacheKeyDenorm, self).setup(**kwargs)

        for dependency in self.depend:
            dependency.setup(self.model)

    def get_triggers(self, using):
        """
        Creates a list of all triggers needed to keep track of changes
        to fields this denorm depends on.
        """
        trigger_list = list()

        # Get the triggers of all DenormDependency instances attached
        # to our callback.
        for dependency in self.depend:
            trigger_list += dependency.get_triggers(using=using)

        return trigger_list + super(BaseCacheKeyDenorm, self).get_triggers(using=using)


class CacheKeyDenorm(BaseCacheKeyDenorm):
    """
    As above, but with extra triggers on self as described below
    """

    def get_triggers(self, using):
        content_type = str(ContentType.objects.get_for_model(self.model).pk)

        # This is only really needed if the instance was changed without
        # using the ORM or if it was part of a bulk update.
        # In those cases the self_save_handler won't get called by the
        # pre_save signal
        action = triggers.TriggerActionUpdate(
            model=self.model,
            columns=(self.fieldname,),
            values=(triggers.RandomBigInt(),),
            where="%s=NEW.%s" % ((self.model._meta.pk.get_attname_column()[1],) * 2),
        )
        trigger_list = [
            triggers.Trigger(self.model, "after", "update", [action], content_type, using, self.skip),
            triggers.Trigger(self.model, "after", "insert", [action], content_type, using, self.skip),
        ]

        return trigger_list + super(CacheKeyDenorm, self).get_triggers(using=using)


class TriggerWhereNode(WhereNode):
    def sql_for_columns(self, data, qn, connection):
        """
        Returns the SQL fragment used for the left-hand side of a column
        constraint (for example, the "T1.foo" portion in the clause
        "WHERE ... T1.foo = 6").
        """
        table_alias, name, db_type = data
        if table_alias:
            if table_alias in ('NEW', 'OLD'):
                lhs = '%s.%s' % (table_alias, qn(name))
            else:
                lhs = '%s.%s' % (qn(table_alias), qn(name))
        else:
            lhs = qn(name)
        return connection.ops.field_cast_sql(db_type) % lhs


class TriggerFilterQuery(sql.Query):
    def __init__(self, model, trigger_alias, where=TriggerWhereNode):
        super(TriggerFilterQuery, self).__init__(model, where)
        self.trigger_alias = trigger_alias

    def get_initial_alias(self):
        return self.trigger_alias

class AggregateDenorm(Denorm):
    __metaclass__ = abc.ABCMeta

    def __init__(self, skip=None):
        self.manager = None
        self.skip = skip

    def setup(self, sender, **kwargs):
        # as we connected to the ``class_prepared`` signal for any sender
        # and we only need to setup once, check if the sender is our model.
        if sender is self.model:
            super(AggregateDenorm, self).setup(sender=sender, **kwargs)

        # related managers will only be available after both models are initialized
        # so check if its available already, and get our manager
        if not self.manager and hasattr(self.model, self.manager_name):
            self.manager = getattr(self.model, self.manager_name)

    def get_related_where(self, fk_name, using, type):
        related_where = ["%s=%s.%s" % (self.model._meta.pk.get_attname_column()[1], type, fk_name)]
        related_query = Query(self.manager.related.model)
        for name, value in self.filter.iteritems():
            related_query.add_q(Q(**{name: value}))
        for name, value in self.exclude.iteritems():
            related_query.add_q(~Q(**{name: value}))
        related_query.add_extra(None, None,
            ["%s=%s.%s" % (self.model._meta.pk.get_attname_column()[1], type, self.manager.related.field.m2m_column_name())],
            None, None, None)
        related_query.add_count_column()
        related_query.clear_ordering(force_empty=True)
        related_query.default_cols = False
        related_filter_where, related_where_params = related_query.get_compiler(using=using,
            connection=connection).as_sql()
        if related_filter_where is not None:
            related_where.append('(' + related_filter_where + ') > 0')
        return related_where, related_where_params

    def m2m_triggers(self, content_type, fk_name, related_field, using):
        """
        Returns triggers for m2m relation
        """
        related_inc_where, _ = self.get_related_where(fk_name, using, 'NEW')
        related_dec_where, related_where_params = self.get_related_where(fk_name, using, 'OLD')
        related_increment = triggers.TriggerActionUpdate(
            model=self.model,
            columns=(self.fieldname,),
            values=(self.get_related_increment_value(),),
            where=(' AND '.join(related_inc_where), related_where_params),
        )
        related_decrement = triggers.TriggerActionUpdate(
            model=self.model,
            columns=(self.fieldname,),
            values=(self.get_related_decrement_value(),),
            where=(' AND '.join(related_dec_where), related_where_params),
        )
        trigger_list = [
            triggers.Trigger(related_field, "after", "update", [related_increment, related_decrement], content_type,
                using,
                self.skip),
            triggers.Trigger(related_field, "after", "insert", [related_increment], content_type, using, self.skip),
            triggers.Trigger(related_field, "after", "delete", [related_decrement], content_type, using, self.skip),
            ]
        return trigger_list

    def get_triggers(self, using):
        related_field = self.manager.related.field
        if isinstance(related_field, ManyToManyField):
            fk_name = related_field.m2m_reverse_name()
            inc_where = ["%(id)s IN (SELECT %(reverse_related)s FROM %(m2m_table)s WHERE %(related)s=NEW.%(id)s)" % {
                'id': self.model._meta.pk.get_attname_column()[0],
                'related': related_field.m2m_column_name(),
                'm2m_table': related_field.m2m_db_table(),
                'reverse_related': fk_name,
                }]
            dec_where = [action.replace('NEW.', 'OLD.') for action in inc_where]
        else:
            fk_name = related_field.attname
            inc_where = ["%s=NEW.%s" % (self.model._meta.pk.get_attname_column()[1], fk_name)]
            dec_where = ["%s=OLD.%s" % (self.model._meta.pk.get_attname_column()[1], fk_name)]

        content_type = str(ContentType.objects.get_for_model(self.model).pk)

        inc_query = TriggerFilterQuery(self.manager.related.model, trigger_alias='NEW')
        inc_query.add_q(Q(**self.filter))
        inc_query.add_q(~Q(**self.exclude))
        inc_filter_where, _ = inc_query.where.as_sql(SQLCompiler(inc_query, connection, using).quote_name_unless_alias,
            connection)
        dec_query = TriggerFilterQuery(self.manager.related.model, trigger_alias='OLD')
        dec_query.add_q(Q(**self.filter))
        dec_query.add_q(~Q(**self.exclude))
        dec_filter_where, where_params = dec_query.where.as_sql(
            SQLCompiler(dec_query, connection, using).quote_name_unless_alias, connection)

        if inc_filter_where:
            inc_where.append(inc_filter_where)
        if dec_filter_where:
            dec_where.append(dec_filter_where)
            # create the triggers for the incremental updates
        increment = triggers.TriggerActionUpdate(
            model=self.model,
            columns=(self.fieldname,),
            values=(self.get_increment_value(),),
            where=(' AND '.join(inc_where), where_params),
        )
        decrement = triggers.TriggerActionUpdate(
            model=self.model,
            columns=(self.fieldname,),
            values=(self.get_decrement_value(),),
            where=(' AND '.join(dec_where), where_params),
        )

        other_model = self.manager.related.model
        trigger_list = [
            triggers.Trigger(other_model, "after", "update", [increment, decrement], content_type, using, self.skip),
            triggers.Trigger(other_model, "after", "insert", [increment], content_type, using, self.skip),
            triggers.Trigger(other_model, "after", "delete", [decrement], content_type, using, self.skip),
            ]
        if isinstance(related_field, ManyToManyField):
            trigger_list.extend(self.m2m_triggers(content_type, fk_name, related_field, using))
        return trigger_list

    @abc.abstractmethod
    def get_increment_value(self):
        """
        Returns SQL for incrementing value
        """

    @abc.abstractmethod
    def get_decrement_value(self):
        """
        Returns SQL for decrementing value
        """

class SumDenorm(AggregateDenorm):
    """
    Handles denormalization of a sum field by doing incrementally updates.
    """
    def __init__(self, skip=None, field = None):
        super(SumDenorm, self).__init__(skip)
        # in case we want to set the value without relying on the
        # correctness of the incremental updates we create a function that
        # calculates it from scratch.
        self.sum_field = field
        self.func = lambda obj: (getattr(obj, self.manager_name).filter(**self.filter).exclude(**self.exclude).aggregate(Sum(self.sum_field)).values()[0] or 0)

    def get_increment_value(self):
        return "%s+NEW.%s" % (self.fieldname, self.sum_field)

    def get_decrement_value(self):
        return "%s-OLD.%s" % (self.fieldname, self.sum_field)

    def get_related_increment_value(self):
        related_query = Query(self.manager.related.model)
        related_query.add_extra(None, None,
            ["%s=%s.%s" % (self.model._meta.pk.get_attname_column()[1], 'NEW', self.manager.related.field.m2m_column_name())],
                                None, None, None)
        related_query.add_fields([self.fieldname])
        related_query.clear_ordering(force_empty=True)
        related_query.default_cols = False
        related_filter_where, related_where_params = related_query.get_compiler(connection=connection).as_sql()
        return "%s + (%s)" % (self.fieldname, related_filter_where)

    def get_related_decrement_value(self):
        related_query = Query(self.manager.related.model)
        related_query.add_extra(None, None,
            ["%s=%s.%s" % (self.model._meta.pk.get_attname_column()[1], 'OLD', self.manager.related.field.m2m_column_name())],
                                None, None, None)
        related_query.add_fields([self.fieldname])
        related_query.clear_ordering(force_empty=True)
        related_query.default_cols = False
        related_filter_where, related_where_params = related_query.get_compiler(connection=connection).as_sql()
        return "%s - (%s)" % (self.fieldname, related_filter_where)

class CountDenorm(AggregateDenorm):
    """
    Handles the denormalization of a count field by doing incrementally
    updates.
    """

    def __init__(self, skip=None):
        super(CountDenorm, self).__init__(skip)
        # in case we want to set the value without relying on the
        # correctness of the incremental updates we create a function that
        # calculates it from scratch.
        self.func = lambda obj: getattr(obj, self.manager_name).filter(**self.filter).exclude(**self.exclude).count()

    def get_increment_value(self):
        return "%s+1" % self.fieldname

    def get_decrement_value(self):
        return "%s-1" % self.fieldname

    def get_related_increment_value(self):
        return self.get_increment_value()

    def get_related_decrement_value(self):
        return self.get_decrement_value()


def rebuildall(verbose=False, model_name=None):
    """
    Updates all models containing denormalized fields.
    Used by the 'denormalize' management command.
    """
    global alldenorms
    for i, denorm in enumerate(alldenorms):
        if model_name is None or denorm.model.__name__ == model_name:
            if verbose:
                print 'rebuilding', '%s/%s' % (i + 1, len(alldenorms)), denorm.fieldname, 'in', denorm.model
            denorm.update(denorm.model.objects.all())


def drop_triggers(using=None):
    triggerset = triggers.TriggerSet(using=using)
    triggerset.drop()


def install_triggers(using=None):
    """
    Installs all required triggers in the database
    """
    build_triggerset(using=using).install()


def build_triggerset(using=None):
    global alldenorms

    # Use a TriggerSet to ensure each event gets just one trigger
    triggerset = triggers.TriggerSet(using=using)
    for denorm in alldenorms:
        triggerset.append(denorm.get_triggers(using=using))
    return triggerset


def flush():
    """
    Updates all model instances marked as dirty by the DirtyInstance
    model.
    After this method finishes the DirtyInstance table is empty and
    all denormalized fields have consistent data.
    """

    # Remove denormalization "locks" that were added
    # more than 15 minutes ago.
    now = datetime.datetime.now()
    since = now - datetime.timedelta(minutes=15)
    DirtyInstance.objects.filter(
        denormalizing_lock_at__isnull=False,
        denormalizing_lock_at__lt=since,
    ).update(
        denormalizing_id=None,
        denormalizing_lock_at=None,
    )

    currently_denormalizing_agents_count = (
        DirtyInstance.objects.
        filter(denormalizing_id__isnull=False).
        distinct('denormalizing_id').count()
    )

    # Getting too crowded?
    if currently_denormalizing_agents_count > 3:
        return

    # Get all content types
    content_types_dict = cache.get('denorm_content_types_dict')
    if content_types_dict is None:
        content_types_dict = {}
        content_types = (
            ContentType.objects.all().
            values('id', 'app_label', 'model')
        )
        for content_type in content_types:
            key = '%s.%s' % (
                content_type['app_label'],
                content_type['model'],
            )
            content_types_dict[key] = content_type
        cache.set('content_types_dict', content_types_dict, 60)

    CONTENT_TYPE_DENORMALIZATION_SEQUENCE = [
        'product.productdenormalized',
        None, # The rest
    ]

    SELECT_RELATED_FOR_CONTENT_TYPE = {
        'product.productdenormalized': [
            'product',
            'product__master_category',
            'product__slave_category__merged_with',
        ],
    }

    denormalizing_id = random.randint(1000000, 9999999)

    # Loop until break.
    # We may need multiple passes, because an update on one instance
    # may cause an other instance to be marked dirty (dependency chains).
    # Process 100 objects at a time.
    while True:

        with transaction.commit_manually():

            objects_denormalized_count = 0

            for content_type_key in CONTENT_TYPE_DENORMALIZATION_SEQUENCE:

                content_type = content_types_dict.get(content_type_key)

                # 1) "Lock" oldest rows (up to 100)

                rows_for_update = DirtyInstance.objects.filter(
                    denormalizing_id=None,
                ).order_by('id')

                if content_type is not None:
                    rows_for_update = rows_for_update.filter(
                        content_type_id=content_type['id'],
                    )

                rows_for_update = rows_for_update[:100]

                DirtyInstance.objects.filter(
                    pk__in=rows_for_update,
                ).update(
                    denormalizing_id=denormalizing_id,
                    denormalizing_lock_at=now,
                )

                transaction.commit()

                # 3) Denormalize objects
                # Call save() on all dirty instances, causing the
                # self_save_handler() getting called by the pre_save signal.

                if content_type is not None:
                    model = get_model(
                        content_type['app_label'],
                        content_type['model'],
                    )

                    qs = model.objects.filter(
                        pk__in=list(DirtyInstance.objects.filter(
                            denormalizing_id=denormalizing_id,
                        ).values_list('object_id', flat=True))
                    )

                    select_related = SELECT_RELATED_FOR_CONTENT_TYPE.get(
                        content_type_key
                    )

                    if select_related is not None:
                        qs = qs.select_related(*select_related)

                    objects = list(qs)

                    transaction.commit()

                    for obj in objects:
                        try:
                            obj.save()
                            # XXX: how to catch errors? TransactionManagementError does not give in
                            # XXX: don't remove objects from DirtyInstance if it errored on save
                        except Exception, e:
                            log.error('Denormalizing error: %s' % e, exc_info=1)
                        else:
                            objects_denormalized_count += 1
                else:
                    qs = DirtyInstance.objects.filter(
                        denormalizing_id=denormalizing_id,
                    ).order_by('id')

                    for dirty_instance in qs:
                        if dirty_instance.content_object:
                            dirty_instance.content_object.save()
                            objects_denormalized_count += 1

                # 4) Delete denormalized objects dirty instance records

                DirtyInstance.objects.filter(
                    denormalizing_id=denormalizing_id,
                ).delete()

                transaction.commit()

            if objects_denormalized_count == 0:
                # Data is consistent
                break
