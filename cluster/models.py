# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from django.utils import timezone

from datetime import datetime, timedelta

# Create your models here.


class Character_category(models.Model):

    id                  = models.BigAutoField(primary_key=True, null=False)
    created_at          = models.DateTimeField(null=True, default=timezone.now)
    last_modified       = models.DateTimeField(null=True, default=timezone.now)
    score               = models.IntegerField(null=True)
    category            = models.BigIntegerField(null=True)
    user_id             = models.BigIntegerField(null=True)
    user_character_id   = models.BigIntegerField(null=True)
    is_closed               = models.BooleanField(null=False, default=False)

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
    is_closed               = models.BooleanField(null=False, default=False)

    class Meta:
        managed = False
        db_table = 'character_theme'


class Character_group(models.Model):
    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    name                    = models.TextField(null=True)
    description             = models.TextField(null=True)
    represent_character_id  = models.BigIntegerField(null=True)
    parent_group_id         = models.BigIntegerField(null=True)

    def __getitem__(self, item):
        return getattr(self, item)

    class Meta:
        managed = False
        db_table = 'character_group'


class Character_schedule(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    score                   = models.IntegerField(null=True)
    schedule                = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)
    user_character_id       = models.BigIntegerField(null=True)
    is_closed               = models.BooleanField(null=False, default=False)

    class Meta:
        managed = False
        db_table = 'character_group'


class User_character(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    age_point               = models.FloatField(null=True)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    character_group_id      = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)
    is_closed               = models.BooleanField(null=False, default=False)

    def __getitem__(self, item):
        return getattr(self, item)

    class Meta:
        managed = False
        db_table = 'user_character'


class Action_log(models.Model):

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


class Rate_log(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    deal_id                 = models.BigIntegerField(null=True)
    point                   = models.IntegerField(null=True, default=0)
    user_id                 = models.BigIntegerField(null=True)


    class Meta:
        managed = False
        db_table = 'rate_log'


class User(models.Model):
    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    nick_name               = models.CharField(max_length=255, unique=True, null=True)
    password                = models.CharField(max_length=255, null=True)
    role                    = models.CharField(max_length=255, null=True)
    user_id                 = models.CharField(max_length=255, unique=True, null=True)
    user_type_id            = models.CharField(max_length=255, null=True)
    category_strings        = models.CharField(max_length=255, null=True)
    gcm_id                  = models.CharField(max_length=255, null=True)
    push_type               = models.IntegerField(default=1, null=True)
    device_id               = models.BigIntegerField(null=True)
    image_url               = models.CharField(max_length=255, null=True)
    version                 = models.CharField(max_length=255, null=True)
    is_night_push_checked   = models.SmallIntegerField(default=0)
    last_logged_in          = models.DateTimeField(null=True, default=timezone.now)

    class Meta:
        managed = False
        db_table = 'user'


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