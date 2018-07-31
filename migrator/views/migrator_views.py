# -*- coding: utf-8 -*-
from __future__ import unicode_literals

#pandas, numpy
import pandas as pd

#json
import json

#regex
import re

#aws s3
import boto3

# datetime
from datetime import datetime

# tz
import pytz

# log
import sys
import traceback
import os

# rest
from rest_framework.decorators import api_view
from rest_framework import status
from rest_framework.response import Response

# model import
from migrator.models import *

# slack message
from slacker import Slacker

# multi-threading
import threading

SLACK_TOKEN = "YOUR_SLACK_TOKEN"
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_KEY_ID"
ENDPOINT = "ap-northeast-2"
TIME_GAP = 9

# preprocess raw log file for analysis and save it to s3
def download_log_from_s3(time_string):
    """
    :param time_string: string of datetime
    :return: list of logs that saved in every minute
    """

    print('thread id : {} download_log_from_s3 START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    s3 = boto3.client('s3', region_name=ENDPOINT,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    log_files_timely = []

    date_time_string = time_string

    print ("TIME : " + date_time_string)

    path ="logs/gcm/"
    bucket = "playwings-log-bucket"
    prefix = path + date_time_string

    # download s3 log file
    try:

        for key in s3.list_objects(Bucket=bucket,Prefix=prefix)['Contents']:

           path_of_object = (key['Key'])
           object_name = (key['Key'])[9:]

           print (object_name)

           log_files_timely.append(object_name)

           s3.download_file(bucket, path_of_object, object_name)

    except KeyError as e:

        print ("PREPROCESS : {} DOESN'T HAVE CONTENTS".format(prefix))
        print('- preprocess_s3_key_error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

    sorted_log =sorted(log_files_timely, key=natural_keys)

    print ("{} / {} JSON FILES COMPLETELY DOWNLOADED".format(len(sorted_log), len(log_files_timely)))
    print('thread id : {} download_log_from_s3 COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return sorted_log

def merge_logs(time_string, list_of_logs):
    """
    :param time_string: string of datetime
    :param list_of_logs: list of logs that saved minutely
    :return: the path of merged log file
    """

    print('thread id : {} merge_logs START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    date_time_string = time_string
    sorted_log = list_of_logs

    # merge every minutes log file into one hourly log file and finally save it zeppelin bucket
    for index, content in enumerate(sorted_log):
        with open(date_time_string + ".json", 'a') as data:
            with open(content, 'r') as f:
                data.write(str(f.read()))
        os.remove(content)
    file_path = date_time_string + ".json"

    print('thread id : {} merge_logs COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return file_path

def transforming_data(path):
    """
    :param path: the path of file that have to transform
    :return: pandas data frame
    """
    print('thread id : {} transforming_data START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    file_path = path

    # read raw merged log file and transform it
    log_table = pd.read_table(file_path, names=["timestamp", "media", "content"], encoding='utf-8')
    log_table["content"] = log_table["content"].apply(lambda x: json.loads(x, encoding='utf-8'))
    log_table["how"] = log_table["content"].apply(lambda x: x["how"])
    log_table["id"] = log_table["content"].apply(lambda x: x["what"]["id"])
    log_table["title"] = log_table["content"].apply(lambda x: x["what"]["title"])
    log_table["type"] = log_table["content"].apply(lambda x: x["what"]["type"])
    log_table["when"] = log_table["content"].apply(lambda x: x["when"])

    print (log_table.shape)

    # drop the column that contains useless data
    log_table.drop(["media", "content"], axis=1, inplace=True)

    # drop former index and reset index
    log_table.reset_index(drop=True, inplace=True)
    print (log_table.shape)

    # group by the result and fill the nan value in dataframe
    log_table_result = log_table.how.groupby([log_table.id, log_table.type]).count().unstack("id").T
    log_table_result.fillna(0, inplace=True)

    print(log_table_result.shape)

    print('thread id : {} transforming_data COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return (file_path, log_table_result)

def update_gcm_log_table(path_of_file, log_table):
    """
    :param path_of_file: path of log file
    :param log_table: data that needs to be written
    :return:
    """

    path = path_of_file

    print('thread id : {} update_gcm_log_table START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    log_dataframe = log_table

    print(log_dataframe.shape)

    print(log_dataframe.tail())

    record_count = 0

    for idx, row in log_dataframe.iterrows():

        try:

            update_gcm_log = Gcm_log.objects.get(id__exact=idx)
            update_gcm_log.view_count += row[0]
            update_gcm_log.click_count += row[1]
            update_gcm_log.last_modified = timezone.now() + timedelta(hours=TIME_GAP)
            # update_gcm_log.save()
            record_count += 1

        except Gcm_log.DoesNotExist:
            print('- update_gcm_log_table DoesNotExist Exception '
                            + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            continue

        except Exception as e:
            print('- update_gcm_log_table Other Exception ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            print(''.join('* ' + line for line in lines))
            continue

    os.remove(path)

    print('thread id : {} update_gcm_log_table COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return record_count

def migrate_main(time_string):
    """
    :param time_string: string of datetime
    :return: result_dict
    """
    print('thread id : {} migrate_main START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    # main function
    slack = Slacker(SLACK_TOKEN)
    channel = "#data_crawler"

    sorted_log = download_log_from_s3(time_string)

    path_of_merged_log = merge_logs(time_string, sorted_log)

    file_path, data_to_be_uploaded = transforming_data(path_of_merged_log)

    record_count = update_gcm_log_table(file_path, data_to_be_uploaded)

    # message a result to slack channel
    slack.chat.post_message(
                            channel,
                            "Migrate" + "\n"
                            + "RECORDS : {}".format(record_count) + "\n"
                            )
    print('thread id : {} migrate_main COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

# if action is searchDeal or searchFlight then save query and keyword
def get_keyword(c):
    if "Query" in c:
        return c[6:]

    elif "Flight" in c:
        return c[7:]

    else:
        return "null"


def atoi(text):
    return int(text) if text.isdecimal() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]

@api_view(['POST'])
def migrator_controller(request):

    if request.method == 'POST':

        time = request.POST.get('time')

        process_th = process_thread(time)
        process_th.start()

        return Response('migrator start', status.HTTP_200_OK)

    else:

        return Response("migrator fail", status.HTTP_500_INTERNAL_SERVER_ERROR)


class process_thread(threading.Thread):

    def __init__(self, time):
        threading.Thread.__init__(self)
        self.time = time

    def run(self):
        migrate_main(self.time)

    def stop(self):
        pass
