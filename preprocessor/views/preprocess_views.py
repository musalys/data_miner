# -*- coding: utf-8 -*-

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

# slack message
from slacker import Slacker

# multi-threading
import threading

SLACK_TOKEN = "YOUR_SLACK_TOKEN"
AWS_ACCESS_KEY_ID = "YOUR_AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_AWS_SECRET_KEY_ID"
ENDPOINT = "ap-northeast-2"

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

    log_timely = []

    date_time_string = time_string

    print ("TIME : " + date_time_string)

    path ="logs/api/"
    bucket = "playwings-log-bucket"
    prefix = path + date_time_string

    # download s3 log file
    try:

        for key in s3.list_objects(Bucket=bucket,Prefix=prefix)['Contents']:

           path_of_object = (key['Key'])
           object_name = (key['Key'])[9:]

           print (object_name)

           log_timely.append(object_name)

           s3.download_file(bucket, path_of_object, object_name)

    except KeyError as e:
        print ("PREPROCESS : {} DOESN'T HAVE CONTENTS".format(prefix))
        print('- preprocess_s3_key_error ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('* ' + line for line in lines))

    sorted_log =sorted(log_timely, key=natural_keys)

    print ("{} / {} JSON FILES COMPLETELY DOWNLOADED".format(len(sorted_log), len(log_timely)))
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
    log_table["userAgent"] = log_table["content"].apply(lambda x: x["who"]["userAgent"])
    log_table["userId"] = log_table["content"].apply(lambda x: x["who"]["userId"])
    log_table["appVersion"] = log_table["content"].apply(lambda x: x["who"]["appVersion"])
    log_table["when"] = log_table["content"].apply(lambda x: x["when"])
    print (log_table.shape)

    # drop the column that contains useless data
    log_table.drop(["content", "media", "appVersion"], axis=1, inplace=True)
    print (log_table.shape)

    # make a column that count id's element in order to discriminate deal detail view from deal list view
    log_table["id_counts"] = log_table.id.apply(lambda x: len(x) if x else 0)

    # make a column that represent user's action categories
    log_table["actionCat"] = log_table.apply(get_action_title, axis=1)

    # make a column that give the weight of actions
    log_table["actionWeight"] = log_table.apply(get_action_weight, axis=1)

    # make a column which can find out what keyword searched by users.
    log_table["actionKeyword"] = log_table.title.apply(get_keyword)

    print (log_table.shape)
    print('thread id : {} transforming_data COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
    return (file_path, log_table)

def upload_transformed_data_to_s3(path_of_file, dataframe):
    """
    :param path: the path of file
    :param dataframe: data that needs to be uploaded
    :return: result summary dictionary
    """
    print('thread id : {} upload_transformed_data_to_s3 START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    file_path = path_of_file
    log_table = dataframe

    s3 = boto3.client('s3', region_name=ENDPOINT,
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # convert preprocessed data to json file
    for idx, row in log_table.iterrows():
        json_of_rows = json.dumps(json.loads(row.to_json(force_ascii=False)), encoding='utf-8')

        with open("c-" + file_path, 'a') as data:
            data.write(json_of_rows)
            data.write('\n')

    print ("LOG : file write done.")

    path = "temp/userlog_daily/"
    bucket = "playwings-log-bucket"
    result_url = path + file_path

    # upload above json file to s3
    s3.upload_file("c-" + file_path, bucket, result_url)

    print ("LOG : s3 upload done.")

    os.remove(file_path)
    # os.remove("c-" + file_path)

    print ("LOG : remove done.")

    # make a result object
    result_dict = {}

    result_dict['url'] = result_url
    result_dict['length'] = len(log_table)
    result_dict['users'] = len(log_table['userId'].unique())
    result_dict['path'] = "c-" + file_path

    print('thread id : {} upload_transformed_data_to_s3 COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return result_dict

def process_main(time_string):
    """
    :param time_string: string of datetime
    :return: result_dict
    """
    print('thread id : {} process_main START '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    # main function
    slack = Slacker(SLACK_TOKEN)
    channel = "#data_crawler"

    sorted_log = download_log_from_s3(time_string)

    path_of_merged_log = merge_logs(time_string, sorted_log)

    file_path, data_to_be_uploaded = transforming_data(path_of_merged_log)

    result_dict = upload_transformed_data_to_s3(file_path, data_to_be_uploaded)

    # message a result to slack channel
    slack.chat.post_message(
                            channel,
                            "Process" + "\n"
                            + "URL : {}".format(result_dict['url']) + "\n"
                            + "RECORDS : {}".format(result_dict['length']) + "\n"
                            + "USERS : {}".format(result_dict['users']) + "\n"
                            + "FILE NAME : {}".format(result_dict['path'])
                            )
    print('thread id : {} process_main COMPLETE '.format(threading.currentThread())
          + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))

    return result_dict

# give a action category title
def get_action_title(value):
    if value.id_counts == 1:
        if value.how == "GET":

            if value.title == "Deal":
                action_title = "getDeal"
                return action_title
            elif value.title == "Comment":
                action_title = "getComment"
                return action_title
            elif value.title == "TiDocument":
                action_title = "getTiDocument"
                return action_title
            elif value.title == "TiComment":
                action_title = "getTiComment"
                return action_title
            elif "Query" in value.title:
                action_title = "searchDeal"
                return action_title
            elif "Flight" in value.title:
                action_title = "searchFlight"
                return action_title

        elif value.how == "LIKE":

            if value.title == "Deal":
                action_title = "likeDeal"
                return action_title
            elif value.title == "Comment":
                action_title = "likeComment"
                return action_title
            elif value.title == "TiDocument":
                action_title = "likeTiDocument"
                return action_title
            elif value.title == "TiComment":
                action_title = "likeTiComment"
                return action_title

        elif value.how == "DISLIKE":

            if value.title == "Deal":
                action_title = "dislikeDeal"
                return action_title
            elif value.title == "Comment":
                action_title = "dislikeComment"
                return action_title
            elif value.title == "TiComment":
                action_title = "dislikeTiComment"
                return action_title

        elif value.how == "CLICK":

            if value.title == "Ad":
                action_title = "clickAd"
                return action_title

        elif value.how == "POST":

            if value.title == "Comment":
                action_title = "postComment"
                return action_title

            elif value.title == "TiDocument":
                action_title = "postTiDocument"
                return action_title

            elif value.title == "TiComment":
                action_title = "postTiComment"
                return action_title

        elif value.how == "MOVE":

            if value.title == "Deal":
                action_title = "moveToDeal"
                return action_title

        elif value.how == "LINK":

            if value.title == "Deal":
                action_title = "sendLink"
                return action_title

        elif value.how == "SHARE":

            if value.title == "Deal":
                action_title = "shareDeal"
                return action_title

        elif value.how == "ORDER":

            if value.title == "Deal_INWEB":
                action_title = "orderDeal"
                return action_title

        elif value.how == "REPORT":

            if value.title == "Deal":
                action_title = "reportDeal"
                return action_title
            elif value.title == "Comment":
                action_title = "reportComment"
                return action_title

        elif value.how == "EXIT":

            if value.title == "Deal":
                action_title = "exitDeal"
                return action_title

        elif value.how == "SCROLL":

            if value.title == "Deal":
                action_title = "scrollDeal"
                return action_title

        else:
            action_title = "etc"
            return action_title

    elif "Query" in value.title:
        action_title = "searchDeal"
        return action_title

    elif "Flight" in value.title:

        if value.how == "MOVE":
            action_title = "clickFlightSearch"
            return action_title

        else:
            action_title = "searchFlight"
            return action_title

    else:
        action_title = "etc"
        return action_title

# give a action weight score
def get_action_weight(value):
    if value.id_counts == 1:
        if value.how == "GET":

            if value.title == "Deal":
                weight = 1
                return weight
            elif value.title == "Comment":
                weight = 0
                return weight
            elif value.title == "TiDocument":
                weight = 0
                return weight
            elif value.title == "TiComment":
                weight = 0
                return weight
            elif "Query" in value.title:
                weight = 7
                return weight
            elif "Flight" in value.title:
                weight = 8
                return weight

        elif value.how == "LIKE":

            if value.title == "Deal":
                weight = 1
                return weight
            elif value.title == "Comment":
                weight = 0
                return weight
            elif value.title == "TiDocument":
                weight = 0
                return weight
            elif value.title == "TiComment":
                weight = 0
                return weight

        elif value.how == "DISLIKE":

            if value.title == "Deal":
                weight = 0
                return weight

            elif value.title == "Comment":
                weight = 0
                return weight
            elif value.title == "TiComment":
                weight = 0
                return weight

        elif value.how == "CLICK":
            if value.title == "Ad":
                weight = 0
                return weight

        elif value.how == "POST":
            if value.title == "Comment":
                weight = 0
                return weight

            elif value.title == "TiDocument":
                weight = 0
                return weight

            elif value.title == "TiComment":
                weight = 0
                return weight

        elif value.how == "SHARE":

            if value.title == "Deal":
                weight = 3
                return weight

        elif value.how == "LINK":

            if value.title == "Deal":
                weight = 4
                return weight

        elif value.how == "MOVE":

            if value.title == "Deal":
                weight = 5
                return weight

        elif value.how == "ORDER":
            if value.title == "Deal_INWEB":
                weight = 6
                return weight

        elif value.how == "REPORT":
            if value.title == "Deal":
                weight = 0
                return weight
            else:
                weight = 0
                return weight

        elif value.how == "EXIT":
            if value.title == "Deal":
                weight = 0
                return weight
        # elif value.how == "SCROLL":
        #     if value.title == "Deal":
        #         weight = 3
        #         return weight

        else:
            weight = 0
            return weight

    elif "Query" in value.title:
        weight = 7
        return weight

    elif "Flight" in value.title:

        if value.how == "MOVE":
            weight = 8
            return weight

        else:
            weight = 10
            return weight

    else:
        weight = 0
        return weight

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
def preprocess_controller(request):

    if request.method == 'POST':

        time = request.POST.get('time')

        process_th = process_thread(time)
        process_th.start()

        return Response('process start', status.HTTP_200_OK)

    else:

        return Response("preprocess fail", status.HTTP_500_INTERNAL_SERVER_ERROR)


class process_thread(threading.Thread):

    def __init__(self, time):
        threading.Thread.__init__(self)
        self.time = time

    def run(self):
        process_main(self.time)

    def stop(self):
        pass
