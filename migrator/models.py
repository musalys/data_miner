# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# timezone
from django.utils import timezone

# time util
from datetime import datetime, timedelta

class Gcm_log(models.Model):

    id                              = models.BigAutoField(primary_key=True, null=False)
    created_at                      = models.DateTimeField(null=True, default=timezone.now)
    last_modified                   = models.DateTimeField(null=True, default=timezone.now)
    deal_id                         = models.BigIntegerField(null=True)
    # except_whole_category           = models.NullBooleanField()
    push_type                       = models.IntegerField(null=False)
    status                          = models.IntegerField(null=False, default='0')
    common_message_id               = models.BigIntegerField(null=True)
    scheduled_at                    = models.DateTimeField(null=True)
    click_count                     = models.IntegerField(default='0')
    view_count                      = models.IntegerField(default='0')
    is_night_push_checked_gcm_log   = models.SmallIntegerField(default='0')
    recommendation_message_id       = models.BigIntegerField(null=True)
    deal_quality                    = models.IntegerField(null=False, default='0')
    is_commerce                     = models.SmallIntegerField(null=False, default='0')
    is_day_push_checked             = models.SmallIntegerField(null=False, default='1')
    is_large                        = models.SmallIntegerField(null=False, default='0')
    is_night_push_checked           = models.SmallIntegerField(null=False, default='0')
    favor_strings                   = models.CharField(max_length=255, null=True)

    class Meta:
        managed = False
        db_table = 'gcm_log'