# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from data_ager.models import *

# rest
from rest_framework.decorators import api_view
from rest_framework import status
from rest_framework.response import Response

# datetime
from datetime import datetime, timedelta
from django.utils import timezone

# tz
import pytz

# log
import sys
import traceback

# threading
import threading

# calculation
from django.db.models import F


def save_aging_log(age_log_data_dict):

    try:

        insert_aging_log = Aginglog(created_at=age_log_data_dict["created_at"],
                                    last_modified=age_log_data_dict["last_modified"], type_id=age_log_data_dict["type_id"],
                                    action_type_id=age_log_data_dict["action_type_id"], user_id=age_log_data_dict["user_id"],
                                    user_character_id=age_log_data_dict["user_character_id"],
                                    gap=age_log_data_dict["gap"])
        insert_aging_log.save()

    except Exception as e:
        print('- save_aging_log error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

def data_aging(age_point, gap_rate):

    AGE_POINT = age_point
    GAP_RATE = gap_rate
    TIME_GAP = 9
    rows = User_character.objects.filter(age_point__exact=0)
    affected_users = []

    if rows.exists():

        for idx, row_1 in enumerate(rows):

            # Entry.objects.filter(mod_date__gt=F('pub_date') + timedelta(days=3))
            # calculate the standard date (row.created_at - 30) and filter by standard date

            user_id = row_1.user_id
            user_character_id = row_1.id

            character_category_result = Character_category.objects.filter(user_id=user_id)

            if character_category_result.exists():

                for idx, row_2 in enumerate(character_category_result):

                    # make aging object and save it to aging_log
                    # data from character_category
                    # if aging rate is 0.8, then gap is 0.2
                    age_log_data_category = {}
                    age_log_data_category["created_at"] = timezone.now() + timedelta(hours=TIME_GAP)
                    age_log_data_category["last_modified"] = age_log_data_category["created_at"]
                    age_log_data_category["type_id"] = row_2.category
                    age_log_data_category["action_type_id"] = 1
                    age_log_data_category["user_id"] = row_2.user_id
                    age_log_data_category["user_character_id"] = user_character_id
                    age_log_data_category["gap"] = int(row_2.score * GAP_RATE)

                    if age_log_data_category["gap"] >= 1:
                        save_aging_log(age_log_data_category)
                try:
                    character_category_result.update(score=F('score') * (1 - GAP_RATE), last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                    affected_users.append(row_2.user_id)
                except Exception as e:
                    print('- data_aging character_category update error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    print(''.join('* ' + line for line in lines))

            character_theme_result = Character_theme.objects.filter(user_id=user_id)

            if character_theme_result.exists():

                for idx, row_3 in enumerate(character_theme_result):
                    age_log_data_theme = {}
                    age_log_data_theme["created_at"] = timezone.now() + timedelta(hours=TIME_GAP)
                    age_log_data_theme["last_modified"] = age_log_data_theme["created_at"]
                    age_log_data_theme["type_id"] = row_3.theme
                    age_log_data_theme["action_type_id"] = 2
                    age_log_data_theme["user_id"] = row_3.user_id
                    age_log_data_theme["user_character_id"] = user_character_id
                    age_log_data_theme["gap"] = int(row_3.score * GAP_RATE)

                    if age_log_data_theme["gap"] >= 1:
                        save_aging_log(age_log_data_theme)

                try:
                    character_theme_result.update(score=F('score') * (1 - GAP_RATE), last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                    affected_users.append(row_3.user_id)
                except Exception as e:
                    print('- data_aging character_theme update error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    print(''.join('* ' + line for line in lines))

        rows.update(age_point=AGE_POINT, last_modified=timezone.now() + timedelta(hours=TIME_GAP))

    else:

        User_character.objects.all().update(age_point=F('age_point') - 1, last_modified=timezone.now() + timedelta(hours=TIME_GAP))
        rows = User_character.objects.filter(age_point__exact=0)

        if rows.exists():

            for idx, row_1 in enumerate(rows):

                # Entry.objects.filter(mod_date__gt=F('pub_date') + timedelta(days=3))
                # calculate the standard date (row.created_at - 30) and filter by standard date

                user_id = row_1.user_id
                user_character_id = row_1.id

                character_category_result = Character_category.objects.filter(user_id=user_id)

                if character_category_result.exists():

                    for idx, row_2 in enumerate(character_category_result):

                        age_log_data_category = {}
                        age_log_data_category["created_at"] = timezone.now() + timedelta(hours=TIME_GAP)
                        age_log_data_category["last_modified"] = age_log_data_category["created_at"]
                        age_log_data_category["type_id"] = row_2.category
                        age_log_data_category["action_type_id"] = 1
                        age_log_data_category["user_id"] = row_2.user_id
                        age_log_data_category["user_character_id"] = user_character_id
                        age_log_data_category["gap"] = int(row_2.score * GAP_RATE)

                        if age_log_data_category["gap"] >= 1:
                            save_aging_log(age_log_data_category)

                    try:
                        character_category_result.update(score=F('score') * (1 - GAP_RATE), last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                        affected_users.append(row_2.user_id)
                    except Exception as e:
                        print('- data_aging character_category update error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                        print(''.join('* ' + line for line in lines))

                character_theme_result = Character_theme.objects.filter(user_id=user_id)

                if character_theme_result.exists():

                    for idx, row_3 in enumerate(character_theme_result):

                        age_log_data_theme = {}
                        age_log_data_theme["created_at"] = timezone.now() + timedelta(hours=TIME_GAP)
                        age_log_data_theme["last_modified"] = age_log_data_theme["created_at"]
                        age_log_data_theme["type_id"] = row_3.theme
                        age_log_data_theme["action_type_id"] = 2
                        age_log_data_theme["user_id"] = row_3.user_id
                        age_log_data_theme["user_character_id"] = user_character_id
                        age_log_data_theme["gap"] = int(row_3.score * GAP_RATE)

                        if age_log_data_theme["gap"] >= 1:
                            save_aging_log(age_log_data_theme)

                    try:
                        character_theme_result.update(score=F('score') * (1 - GAP_RATE), last_modified=timezone.now() + timedelta(hours=TIME_GAP))
                        affected_users.append(row_3.user_id)
                    except Exception as e:
                        print('- data_aging character_theme update error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                        print(''.join('* ' + line for line in lines))

            rows.update(age_point=AGE_POINT, last_modified=timezone.now() + timedelta(hours=TIME_GAP))

    print ("Aging : Aging process is done.")


@api_view(['POST'])
def aging_controller(request):

    if request.method == 'POST':

        age_point = int(request.POST.get('age_point'))
        gap_rate = float(request.POST.get('gap_rate'))

        try:
            thread = Aging_thread(age_point, gap_rate)
            thread.setDaemon(True)
            thread.start()

            return Response("aging success", status=status.HTTP_200_OK)

        except Exception as e:
            print('- aging_controller POST error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return Response("aging fail", status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class Aging_thread(threading.Thread):
    def __init__(self, age_point, gap_rate):
        threading.Thread.__init__(self)
        self.age_point = age_point
        self.gap_rate = gap_rate

    def run(self):
        data_aging(self.age_point, self.gap_rate)
