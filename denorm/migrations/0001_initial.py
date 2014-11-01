# -*- coding: utf-8 -*-
from south.utils import datetime_utils as datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'DirtyInstance'
        db.create_table(u'denorm_dirtyinstance', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('content_type', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['contenttypes.ContentType'])),
            ('object_id', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('denormalizing_id', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('denormalizing_lock_at', self.gf('django.db.models.fields.DateTimeField')(null=True, blank=True)),
        ))
        db.send_create_signal(u'denorm', ['DirtyInstance'])

        # Adding unique constraint on 'DirtyInstance', fields ['content_type', 'object_id']
        db.create_unique(u'denorm_dirtyinstance', ['content_type_id', 'object_id'])


    def backwards(self, orm):
        # Removing unique constraint on 'DirtyInstance', fields ['content_type', 'object_id']
        db.delete_unique(u'denorm_dirtyinstance', ['content_type_id', 'object_id'])

        # Deleting model 'DirtyInstance'
        db.delete_table(u'denorm_dirtyinstance')


    models = {
        u'contenttypes.contenttype': {
            'Meta': {'ordering': "('name',)", 'unique_together': "(('app_label', 'model'),)", 'object_name': 'ContentType', 'db_table': "'django_content_type'"},
            'app_label': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'model': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '100'})
        },
        u'denorm.dirtyinstance': {
            'Meta': {'unique_together': "(('content_type', 'object_id'),)", 'object_name': 'DirtyInstance'},
            'content_type': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['contenttypes.ContentType']"}),
            'denormalizing_id': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'denormalizing_lock_at': ('django.db.models.fields.DateTimeField', [], {'null': 'True', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'object_id': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'})
        }
    }

    complete_apps = ['denorm']