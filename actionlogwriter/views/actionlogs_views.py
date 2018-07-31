# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from actionlogwriter.models import *

# rest
from rest_framework.decorators import api_view
from rest_framework import status
from rest_framework.response import Response

# aws s3
import boto3

# pandas
import pandas as pd

# datetime
from datetime import datetime
from django.utils import timezone

# base64
import base64

# tz
import pytz

# log
import sys
import traceback
import os

# threading
import threading

# queue
import Queue

from dateutil.parser import parse

# uuid
import uuid

# time
import time

# slack
from slacker import Slacker

SLACK_TOKEN = "YOUR_SLACK_TOKEN"
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_KEY_ID"
ENDPOINT = "ap-northeast-2"

ACTION_TYPE_CATEGORY = 1
ACTION_TYPE_THEME = 2
TIME_GAP = 9


def decode_userid(unicode_text):
    decoded_text = base64.decodestring(unicode_text)

    try:
        return int(decoded_text.strip())
    except Exception as e:
        print (unicode_text)
        print('- decode_userid ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        return 0


def save_action_log_to_character_mysql(data):

    print('thread id : {} save_action_log_to_character_mysql START '.format(threading.currentThread()))

    global TIME_GAP

    if data.action_type == "category":

        action_log_type_id = data.type

        if action_log_type_id != 0 and action_log_type_id != None:

            try:

                update_character_category = Character_category.objects.get(
                    user_character_id__exact=data.user_character_id,
                    user_id__exact=data.user_id,
                    category__exact=action_log_type_id)

                update_character_category.score += data.weight
                update_character_category.last_modified = timezone.now() + timedelta(hours=TIME_GAP)
                update_character_category.save()

                print ("LOG : CHARACTER CATEGORY SUCCESSFULLY UPDATED TO ACTION_LOG MYSQL.")

            except Character_category.DoesNotExist:

                # if record does not exist, create model object and save record to db
                update_character_category = Character_category(
                    created_at=data.created_at,
                    last_modified=timezone.now() + timedelta(hours=TIME_GAP),
                    score=data.weight,
                    category=action_log_type_id,
                    user_id=data.user_id,
                    user_character_id=data.user_character_id
                )
                update_character_category.save()

                print ("LOG : CHARACTER CATEGORY SUCCESSFULLY SAVED TO ACTION_LOG MYSQL.")

            except Character_category.MultipleObjectsReturned:

                # if query return 2 or more object, then try force update into first record
                update_character_category = Character_category.objects.filter(
                    user_character_id__exact=data.user_character_id,
                    user_id__exact=data.user_id,
                    category__exact=action_log_type_id) \
                    .order_by('-last_modified') \
                    .first()
                update_character_category.score += data.weight
                update_character_category.last_modified = timezone.now() + timedelta(hours=TIME_GAP)
                update_character_category.save()

                print ("LOG : CHARACTER CATEGORY SUCCESSFULLY UPDATED INTO FIRST AMONG RECORDS.")

            except Exception as e:
                print ("=========================================================================================")
                print ("USER_ID : {}".format(data.user_id))
                print ("USER_CHARACTER_ID : {}".format(data.user_character_id))
                print ("CATEGORY : {}".format(action_log_type_id))
                print ("SCORE : {}".format(data.weight))
                print ("CREATED_AT : {}".format(data.created_at))
                print ("LAST_MODIFIED : {}".format(timezone.now() + timedelta(hours=TIME_GAP)))
                print ("=========================================================================================")
                print('- save_action_log_to_character_mysql ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))

        else:
            print ("ACTION_LOG_TYPE_ID : {}".format(action_log_type_id))
            print ("LOG : category id is 0 or None")

    elif data.action_type == "theme":

        action_log_type_id = data.type

        if action_log_type_id != 0 and action_log_type_id != None:

            try:

                # query existing record, if query returns only one record, then update and save record
                update_character_theme = Character_theme.objects.get(
                    user_character_id__exact=data.user_character_id,
                    user_id__exact=data.user_id,
                    theme__exact=action_log_type_id)

                update_character_theme.score += data.weight
                update_character_theme.last_modified = timezone.now() + timedelta(hours=TIME_GAP)
                update_character_theme.save()

                print ("LOG : CHARACTER THEME SUCCESSFULLY UPDATED TO ACTION_LOG MYSQL.")

            except Character_theme.DoesNotExist:

                # if record does not exist, create model object and save record to db
                update_character_theme = Character_theme(
                    created_at=data.created_at,
                    last_modified=timezone.now() + timedelta(hours=TIME_GAP),
                    score=data.weight,
                    theme=action_log_type_id,
                    user_id=data.user_id,
                    user_character_id=data.user_character_id
                )
                update_character_theme.save()

                print ("LOG : CHARACTER THEME SUCCESSFULLY SAVED TO ACTION_LOG MYSQL.")

            except Character_theme.MultipleObjectsReturned:

                # if query return 2 or more object, then try force update into first record
                update_character_theme = Character_theme.objects.filter(
                    user_character_id__exact=data.user_character_id,
                    user_id__exact=data.user_id,
                    theme__exact=action_log_type_id) \
                    .order_by('-last_modified') \
                    .first()
                update_character_theme.score += data.weight
                update_character_theme.last_modified = timezone.now() + timedelta(hours=TIME_GAP)
                update_character_theme.save()

                print ("LOG : CHARACTER THEME SUCCESSFULLY UPDATED INTO FIRST AMONG RECORDS.")

            except Exception as e:
                print ("=========================================================================================")
                print ("USER_ID : {}".format(data.user_id))
                print ("USER_CHARACTER_ID : {}".format(data.user_character_id))
                print ("THEME : {}".format(action_log_type_id))
                print ("SCORE : {}".format(data.weight))
                print ("CREATED_AT : {}".format(data.created_at))
                print ("LAST_MODIFIED : {}".format(timezone.now() + timedelta(hours=TIME_GAP)))
                print ("=========================================================================================")
                print('- save_action_log_to_character_mysql ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))

        else:
            print ("ACTION_LOG_TYPE_ID : {}".format(action_log_type_id))
            print ("LOG : theme id is 0 or None")

    else:
        print ("There's No data")
        return None

    print('thread id : {} save_action_log_to_character_mysql COMPLETE '.format(threading.currentThread()))


def save_action_log_to_character_dynamo(datas):

    print('thread id : {} save_action_log_to_character_dynamo START '.format(threading.currentThread()))

    for data in datas:

        action_log = ActionLog(
                                user_character_id=data["user_character_id"],
                                hash_code=str(uuid.uuid4()),
                                user_id=data["user_id"],
                                action_type=data["action_type_id"],
                                type=data["category_or_theme_id"],
                                weight=data["weight"],
                                created_at=data["created_at"],
                                last_modified=data["last_modified"]
        )

        save_action_log_to_character_mysql(action_log)

        try:
            action_log.save()
            print ("LOG : SUCCESSFULLY WRITE COMPLETED INTO DYNAMODB.")

        except Exception as e:
            print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            continue

    print("length of bulk object is {}".format(len(datas)))
    print('thread id : {} save_action_log_to_character_dynamo COMPLETE '.format(threading.currentThread()))


def save_action_log_to_mysql_ratelog(data, action_name):

    print('thread id : {} save_action_log_to_mysql_ratelog START '.format(threading.currentThread()))

    # positive indirect indices
    SCROLL_RATE_LOG_POINT = 3
    LIKE_RATE_LOG_POINT = 3
    SENDLINK_OR_SHARE_RATE_LOG_POINT = 4
    CONV_RATE_LOG_POINT = 4
    INWEB_PAYMENT_RATE_LOG_POINT = 5

    # negative indirect indices
    EXIT_RATE_LOG_POINT = 2
    DISLIKE_RATE_LOG_POINT = 2
    REPORT_RATE_LOG_POINT = 1

    # make rate_log type indirect
    RATE_LOG_TYPE = "INDIRECT"

    if (data["category_or_theme_id"] != 0) and (data["category_or_theme_id"] != None):

        try:

            actionlog_to_mysql_ratelog = Rate_log(
                created_at=data["created_at"],
                deal_id=data["category_or_theme_id"],
                last_modified=data["last_modified"],
                point=None,
                user_id=data["user_id"],
                type=RATE_LOG_TYPE
            )

        except Exception as e:
            print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            return

        # positive indirect actions
        if action_name == "scrollDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', SCROLL_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        elif action_name == "likeDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', LIKE_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        elif (action_name == "shareDeal") or (action_name == "sendLink"):
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', SENDLINK_OR_SHARE_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        elif action_name == "moveToDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', CONV_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        elif action_name == "orderDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', INWEB_PAYMENT_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        # negative indirect actions
        elif action_name == "dislikeDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', DISLIKE_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        elif action_name == "exitDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', EXIT_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return

        elif action_name == "reportDeal":
            try:
                setattr(actionlog_to_mysql_ratelog, 'point', REPORT_RATE_LOG_POINT)
            except Exception as e:
                print('- save_action_log_to_mysql_ratelog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))
                return


        else:
            actionlog_to_mysql_ratelog = None
            return

        actionlog_to_mysql_ratelog.save()
        print ("ACTION LOG SUCCESSFULLY SAVED TO RATE LOG.")

    else:
        print ("ACTION LOG NOT SAVED TO RATE LOG : DEAL ID IS {}".format(data["category_or_theme_id"]))
        print ("NO RATE LOG DATA.")
        return

    print('thread id : {} save_action_log_to_mysql_ratelog COMPLETE '.format(threading.currentThread()))


def save_conversion_log_to_mysql_actionlog(data):

    print('thread id : {} save_conversion_log_to_mysql_actionlog START '.format(threading.currentThread()))

    if (data["category_or_theme_id"]) != 0 and (data["category_or_theme_id"] != None):

        try:
            conversion_log_to_mysql = Actionlog(
                created_at=data["created_at"],
                last_modified=data["last_modified"],
                type_id=data["category_or_theme_id"],
                weight=data["weight"],
                action_type_id=1,
                user_id=data["user_id"],
                user_character_id=data["user_character_id"],
            )

            conversion_log_to_mysql.save()

        except Exception as e:

            print('- save_conversion_log_to_mysql_actionlog ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))

    else:
        print ("DEAL ID IN CONVERSION LOG IS {}".format(data["category_or_theme_id"]))
        print ("CONVERSION LOG NOT SAVED : DEAL ID IS NONE OR 0")

    print('thread id : {} save_conversion_log_to_mysql_actionlog COMPLETE '.format(threading.currentThread()))


def category_id_converter(category_id):
    if category_id != 9:
        rows = Category.objects.filter(id__exact=category_id)

        if rows.exists():
            for idx, row in enumerate(rows):
                return row.parent_category_id

        else:
            return category_id

    else:
        return category_id


def get_user_character_id_by_user_id(user_id):
    try:
        rows = User_character.objects.filter(user_id__exact=user_id, is_closed=False)
        result_row = rows.first()

        if result_row:
            return result_row.id
        else:
            raise Exception('no such user_id in user_character table!')

    except Exception as e:
        print('- get_user_character_id_by_user_id ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        return None


def get_arrival_id_by_deal_id_for_category(data, given_deal_id):
    try:
        rows = Ticket.objects.filter(deal_id=given_deal_id)

        if rows.exists():

            arrival_ids = []

            for _, row in enumerate(rows):

                if not row.arrival_id in arrival_ids:
                    arrival_ids.append(row.arrival_id)
                else:
                    continue

            return get_area_category_by_arrival_id(data, arrival_ids)

        else:
            raise Exception("can't find any arrival id for given deal id")

    except Exception as e:
        print('- get_arrival_id_by_deal_id_for_category ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

        object_list = []
        data["category_or_theme_id"] = 0
        data["action_type_id"] = "category"
        object_list.append(data)
        return object_list


def get_area_category_by_arrival_id(data, arrival_ids):
    try:
        rows = Area.objects.filter(id__in=arrival_ids)

        if rows.exists():

            category_ids = []

            for _, row in enumerate(rows):

                if not row.category_id in category_ids:
                    category_ids.append(row.category_id)
                else:
                    continue

            object_list = []

            for cat_id in category_ids:
                data_copy = data.copy()
                data_copy["category_or_theme_id"] = cat_id
                data_copy["action_type_id"] = "category"

                object_list.append(data_copy)

            return object_list

        else:
            raise Exception

    except Exception as e:
        print('- get_area_category_by_arrival_id ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

        object_list = []
        data["cateogry_or_theme_id"] = 0
        data["action_type_id"] = "category"
        object_list.append(data)
        return object_list


def get_deal_area_category_by_keyword(data, value):
    try:
        rows = Area.objects.filter(country_name__icontains=value.encode('utf-8'))

        if rows.exists():

            object_list = []
            category_ids = []

            for _, row in enumerate(rows):

                if not row.category_id in category_ids:
                    category_ids.append(row.category_id)
                else:
                    continue

            for category_id in category_ids:
                data_copy = data.copy()
                data_copy["category_or_theme_id"] = category_id
                data_copy["action_type_id"] = "category"

                object_list.append(data_copy)

            return object_list

        else:
            rows = Area.objects.filter(city_name__icontains=value.encode('utf-8'))
            result_row = rows.first()

            if result_row:

                object_list = []

                data_copy = data.copy()
                data_copy["category_or_theme_id"] = result_row.category_id
                data_copy["action_type_id"] = "category"

                object_list.append(data_copy)

                return object_list

            else:
                raise Exception

    except Exception as e:
        print('- get_deal_area_category_by_keyword ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

        object_list = []
        data["cateogry_or_theme_id"] = 0
        data["action_type_id"] = "category"
        object_list.append(data)
        return object_list


def get_flight_area_category_by_keyword(data, value):
    try:
        rows = Area.objects.filter(airport_code__icontains=value)

        if rows.exists():

            object_list = []

            for _, row in enumerate(rows):
                data_copy = data.copy()
                data_copy["category_or_theme_id"] = row.category_id
                data_copy["action_type_id"] = "category"

                object_list.append(data_copy)

            return object_list

        else:
            raise Exception

    except Exception as e:
        print('- get_flight_area_category_by_keyword ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        object_list = []
        data["cateogry_or_theme_id"] = 0
        data["action_type_id"] = "category"
        object_list.append(data)
        return object_list


def get_arrival_id_by_deal_id_for_theme(data, given_deal_id):
    try:
        rows = Ticket.objects.filter(deal_id=given_deal_id)

        if rows.exists():

            arrival_ids = []

            for _, row in enumerate(rows):

                if not row.arrival_id in arrival_ids:
                    arrival_ids.append(row.arrival_id)
                else:
                    continue

            return get_area_theme_by_arrival_id(data, arrival_ids)

        else:
            raise Exception("can't find any arrival id for given deal id")

    except Exception as e:
        print('- get_arrival_id_by_deal_id_for_theme ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        object_list = []
        data["category_or_theme_id"] = 0
        data["action_type_id"] = "theme"
        object_list.append(data)
        return object_list


def get_area_theme_by_arrival_id(data, arrival_ids):
    try:
        rows = Area_themes.objects.filter(area_id__in=arrival_ids)

        if rows.exists():

            theme_ids = []

            for _, row in enumerate(rows):

                if not row.theme_id in theme_ids:
                    theme_ids.append(row.theme_id)
                else:
                    continue

            object_list = []

            for theme_id in theme_ids:
                data_copy = data.copy()
                data_copy["category_or_theme_id"] = theme_id
                data_copy["action_type_id"] = "theme"
                object_list.append(data_copy)

            return object_list

        else:
            raise Exception("can't find any theme id for given arrival id")

    except Exception as e:
        print('- get_area_theme_by_arrival_id ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        object_list = []
        data["category_or_theme_id"] = 0
        data["action_type_id"] = "theme"
        object_list.append(data)
        return object_list


def get_deal_area_theme_by_keyword(data, value):
    try:
        rows = Area.objects.filter(city_name__icontains=value.encode('utf-8'))

        if rows.exists():

            arrival_ids = []

            for _, row in enumerate(rows):

                if not row.id in arrival_ids:
                    arrival_ids.append(row.id)
                else:
                    continue

            return get_area_theme_by_arrival_id(data, arrival_ids)

        else:
            raise NameError('Wrong Keyword or Country Name')

    except Exception as e:
        print('- get_deal_area_theme_by_keyword ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        object_list = []
        return object_list


def get_flight_area_theme_by_keyword(data, value):
    try:
        rows = Area.objects.filter(airport_code__icontains=value)

        if rows.exists():

            arrival_ids = []

            for _, row in enumerate(rows):

                if not row.id in arrival_ids:
                    arrival_ids.append(row.id)
                else:
                    continue

            return get_area_theme_by_arrival_id(data, arrival_ids)
        else:
            raise NameError('No Data!')

    except Exception as e:
        print(value)
        print('- get_flight_area_theme_by_keyword ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        object_list = []
        data["category_or_theme_id"] = 0
        data["action_type_id"] = "theme"
        object_list.append(data)
        return object_list


def save_log_list_into_database(result_list):

    print('thread id : {} save_log_list_into_database START '.format(threading.currentThread()))
    result_list_len = 5000
    split_result_list = [result_list[x:x + result_list_len] for x in xrange(0, len(result_list), result_list_len)]

    def save_list_into_database(*args):
        for idx, data in enumerate(args[0]):
            save_action_log_to_character_dynamo(data)
            time.sleep(10)

    for split_list in split_result_list:
        split_list_nested = [split_list]
        thread = threading.Thread(target=save_list_into_database, args=(split_list_nested,))
        thread.daemon = True
        thread.start()
        thread.join()

    print('thread id : {} save_log_list_into_database COMPLETE '.format(threading.currentThread()))


def handle_logs_into_list(log_df_filtered):

    print('thread id : {} handle_logs_into_list START '.format(threading.currentThread()))


    filtered_log_data_frame = log_df_filtered

    result_list = []

    for idx, row in filtered_log_data_frame.iterrows():

        temp_data = {}
        temp_data["user_id"] = row["userId"]
        temp_data["created_at"] = row["when"]
        temp_data["last_modified"] = temp_data["created_at"]
        temp_data["category_or_theme_id"] = 0
        temp_data["weight"] = row["actionWeight"]
        temp_data["action_type_id"] = "None"
        temp_data["user_character_id"] = get_user_character_id_by_user_id(row["userId"])

        if (row["actionCat"] == "searchDeal") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_deal_area_category_by_keyword(temp_data, row["actionKeyword"])

            result_list += object_list_for_category

        elif (row["actionCat"] == "searchFlight") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_flight_area_category_by_keyword(temp_data, row["actionKeyword"][-3:])

            result_list += object_list_for_category

        elif (row["actionCat"] == "clickFlightSearch") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_flight_area_category_by_keyword(temp_data, row["actionKeyword"][-3:])

            result_list += object_list_for_category

        elif (row["actionCat"] == "getDeal") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

        elif (row["actionCat"] == "likeDeal") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "scrollDeal") and (temp_data["user_character_id"] != None):
            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "moveToDeal") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # save conversion log to mysql action log
            ## replace category id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            temp_data["action_type_id"] = 1
            save_conversion_log_to_mysql_actionlog(temp_data)
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "shareDeal") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "sendLink") and (temp_data["user_character_id"] != None):

            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "orderDeal") and (temp_data["user_character_id"] != None):
            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            ## replace category id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "reportDeal") and (temp_data["user_character_id"] != None):
            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "exitDeal") and (temp_data["user_character_id"] != None):
            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        elif (row["actionCat"] == "dislikeDeal") and (temp_data["user_character_id"] != None):
            object_list_for_category = get_arrival_id_by_deal_id_for_category(temp_data, row["id"][0])

            result_list += object_list_for_category

            # replace category or theme type id with deal id
            temp_data["category_or_theme_id"] = row["id"][0]
            save_action_log_to_mysql_ratelog(temp_data, row["actionCat"])

        else:

            print(temp_data)
            continue

    print('thread id : {} handle_logs_into_list COMPLETE '.format(threading.currentThread()))

    # transform list of dict to dataframe and filter 0 (useless & unknown) values
    try:
        result_list_2 = pd.DataFrame(result_list)
        print("original shape {}".format(result_list_2.shape))

        result_list_2 = result_list_2[(result_list_2["category_or_theme_id"] != 0) & (result_list_2["weight"] != 0)]
        print("modified shape after filtering {}".format(result_list_2.shape))

        result_list_2 = result_list_2.to_dict('records')
        print("final shape {}".format(len(result_list_2)))


    except Exception as e:

        print('- actionlog handle_logs_into_list error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

    return result_list_2


def open_json_file(path):

    print('thread id : {} open_json_file START '.format(threading.currentThread()))

    file_name = path

    try:

        json_logs_to_dataframe = pd.read_json(file_name, orient='records', lines=True, encoding='utf-8')

    except Exception as e:

        print ("ACTIONLOG : {} DOESN'T HAVE CONTENTS".format(file_name))
        print('- actionlog open_json_file error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))
        return None

    print('thread id : {} open_json_file COMPLETE '.format(threading.currentThread()))

    return json_logs_to_dataframe

def preprocess_data(data_frame):
    """
    :param data_frame: pandas dataframe that just opened and transformed before.
    :return: pre-processed dataframe
    """
    print('thread id : {} preprocess_data START '.format(threading.currentThread()))

    logs_df = data_frame

    logs_df["userId"] = logs_df["userId"].apply(lambda x: decode_userid(x))
    logs_df["when"] = logs_df["when"].apply(lambda x: parse(x) + timedelta(hours=9))

    print ("ORIGINAL RAW LOG LENGTH IS {}".format(logs_df.shape))

    filtered_logs = logs_df[(logs_df["actionWeight"] != 0) & (logs_df["userId"] != 0)]
    filtered_logs.reset_index(drop=True, inplace=True)

    print("NO WEIGHT AND NONE USER ID ROWS EXCLUDED. SO THE SIZE OF FILTERED DATA IS {}.".format(filtered_logs.shape))
    print('thread id : {} preprocess_data COMPLETE '.format(threading.currentThread()))

    return filtered_logs

# main
def actionlog_main(path):
    """
    :param path: path of the preprocessed file
    :return: http status code
    """

    print('- ACTIONLOG WRITE START ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
    print('thread id : {} actionlog_main START '.format(threading.currentThread()))

    slack = Slacker(SLACK_TOKEN)
    channel = "#data_crawler"

    file_path = path

    print(file_path)

    # open json file and transform it to pandas dataframe
    data_frame = open_json_file(file_path)

    # transform and manipulate data by pandas
    filtered_data_frame = preprocess_data(data_frame)

    # handle logs
    handled_logs_list = handle_logs_into_list(filtered_data_frame)

    save_log_list_into_database(handled_logs_list)

    print ("LOG : save log to mysql is completed.")

    os.remove(file_path)

    result_dict = {}
    result_dict['path'] = file_path
    result_dict['length'] = len(filtered_data_frame)
    result_dict['user'] = len(filtered_data_frame['userId'].unique())

    slack.chat.post_message(
                                channel,
                                "Actionlog" + "\n"
                                + "RECORDS : {}".format(result_dict['length']) + "\n"
                                + "USERS : {}".format(result_dict['user']) + "\n"
                                + "FILE NAME : {}".format(result_dict['path'])
    )

    print('thread id : {} actionlog_main COMPLETE '.format(threading.currentThread()))
    print('- ACTIONLOG WRITE COMPLETED ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return result_dict


@api_view(['POST'])
def actionlogs_controller(request):
    if request.method == 'POST':

        path = request.POST.get('path')

        try:

            thread = actionlogs_thread(path)
            thread.start()

            return Response("actionlog start", status=status.HTTP_200_OK)

        except Exception as e:
            print('- actionlog_controller POST error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))


class actionlogs_thread(threading.Thread):

    def __init__(self, path):

        threading.Thread.__init__(self)
        self.path = path

    def run(self):

        actionlog_main(self.path)

    def stop(self):
        pass
