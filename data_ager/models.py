# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from django.utils import timezone

# Create your models here.

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


    class Meta:
        managed = False
        db_table = 'character_schedule'


class User_character(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    age_point               = models.FloatField(null=True)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    character_group_id      = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)


    class Meta:
        managed = False
        db_table = 'user_character'


class Aginglog(models.Model):

    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    type_id                 = models.BigIntegerField(null=True)
    gap                     = models.IntegerField(null=True, default=0)
    action_type_id          = models.BigIntegerField(null=True)
    user_id                 = models.BigIntegerField(null=True)
    user_character_id       = models.BigIntegerField(null=True)

    class Meta:
        managed = False
        db_table = 'aging_log'



class Character_group(models.Model):
    id                      = models.BigAutoField(primary_key=True, null=False)
    created_at              = models.DateTimeField(null=True, default=timezone.now)
    last_modified           = models.DateTimeField(null=True, default=timezone.now)
    represent_character_id  = models.BigIntegerField(null=True)
    parent_group_id         = models.BigIntegerField(null=True)


    class Meta:
        managed = False
        db_table = 'character_group'
