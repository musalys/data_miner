# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from django.utils import timezone

from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, UTCDateTimeAttribute
)

from pynamodb.indexes import LocalSecondaryIndex, AllProjection

from datetime import datetime, timedelta

class Category(models.Model):
    id                  = models.BigAutoField(primary_key=True, null=False)
    name                = models.CharField(max_length=255, null=True)
    description         = models.CharField(max_length=255, null=True)
    image_url           = models.CharField(max_length=255, null=True)
    using_off           = models.BooleanField(null=False)
    parent_category_id  = models.BigIntegerField(null=True)
    code                = models.CharField(max_length=255, null=True)

    class Meta:
        managed = False
        db_table = 'category'


class Ticket(models.Model):

    id                  = models.BigAutoField(primary_key=True, null=False)
    created_at          = models.DateTimeField(null=True, default=timezone.now)
    deal_id             = models.BigIntegerField(null=True)
    end_date            = models.DateTimeField(null=True, default=timezone.now)
    last_modified       = models.DateTimeField(null=True, default=timezone.now)
    price               = models.FloatField(null=False)
    start_date          = models.DateTimeField(null=True, default=timezone.now)
    arrival_id          = models.BigIntegerField(null=True)
    departure_id        = models.BigIntegerField(null=False)
    ord                 = models.IntegerField(null=False)
    one_way_price       = models.FloatField(null=True)

    class Meta:
        managed = False
        db_table = 'ticket'


class Area(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    airport_code            = models.CharField(max_length=255, null=True)
    airport_name            = models.CharField(max_length=255, null=True)
    city_code               = models.CharField(max_length=255, null=True)
    city_name               = models.CharField(max_length=255, null=True)
    country_code            = models.CharField(max_length=255, null=True)
    country_name            = models.CharField(max_length=255, null=True)
    category_id             = models.BigIntegerField(null=True)
    area_dangerous          = models.BooleanField(null=False)
    area_popular            = models.BooleanField(null=False)
    area_popular_ord        = models.IntegerField(null=False)

    class Meta:
        managed = False
        db_table = 'area'


class Actionlog(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    type_id                 = models.BigIntegerField(null=True)
    weight                  = models.IntegerField(null=True, default=0)
    action_type_id          = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)
    user_character_id       = models.BigIntegerField(null=True)

    class Meta:
        managed = False
        db_table = 'action_log'


class Area_themes(models.Model):
    area_id                         = models.BigIntegerField(primary_key=True, null=False)
    theme_id                        = models.BigIntegerField(primary_key=True, null=False)

    class Meta:
        managed = False
        db_table = 'area_themes'


class User_character(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    age_point               = models.FloatField(null=True)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    character_group_id      = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)
    is_closed               = models.BooleanField(null=False, default=False)

    class Meta:
        managed = False
        db_table = 'user_character'


class Character_category(models.Model):

    id                  = models.BigAutoField(primary_key=True, null=False)
    created_at          = models.DateTimeField(null=True, default=timezone.now)
    last_modified       = models.DateTimeField(null=True, default=timezone.now)
    score               = models.IntegerField(null=True)
    category            = models.BigIntegerField(null=True)
    user_id             = models.BigIntegerField(null=True)
    user_character_id   = models.BigIntegerField(null=True)


    class Meta:
        managed = False
        db_table = 'character_category'


class Character_theme(models.Model):

    id                  = models.BigAutoField(primary_key=True, null=False)
    created_at          = models.DateTimeField(null=True, default=timezone.now)
    last_modified       = models.DateTimeField(null=True, default=timezone.now)
    score               = models.IntegerField(null=True)
    theme               = models.BigIntegerField(null=True)
    user_id             = models.BigIntegerField(null=True)
    user_character_id   = models.BigIntegerField(null=True)


    class Meta:
        managed = False
        db_table = 'character_theme'


class Character_schedule(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    score                   = models.IntegerField(null=True)
    schedule                = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)
    user_character_id       = models.BigIntegerField(null=True)


class Rate_log(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    deal_id                 = models.BigIntegerField(null=True)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    point                   = models.IntegerField(null=True, default=0)
    user_id                 = models.BigIntegerField(null=True)
    type                    = models.CharField(max_length=255, null=True)


    class Meta:
        managed = False
        db_table = 'rate_log'


class ActionLog(Model):

    class Meta:
        table_name = 'ActionLog'
        # Specifies the region
        region = 'ap-northeast-2'
        # Specifies the write capacity
        write_capacity_units = 10
        # Specifies the read capacity
        read_capacity_units = 10

    user_character_id = NumberAttribute(hash_key=True)
    hash_code = UnicodeAttribute(range_key=True)

    user_id = NumberAttribute()
    action_type = UnicodeAttribute()
    type = NumberAttribute()
    weight = NumberAttribute(default=0)

    created_at = UTCDateTimeAttribute()
    last_modified = UTCDateTimeAttribute()
