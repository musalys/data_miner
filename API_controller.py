# -*- coding: utf-8 -*-

# datetime
from datetime import datetime, timedelta

# request
import requests

# slack
from slacker import Slacker


class API_controller:

    channel = '#data_crawler'
    before = ''
    check_time = 0
    slack_token = 'YOUR_SLACK_TOKEN'

    def __init__(self):
        self.slack = Slacker(slack_token)

    def send_post_request_to_process(self):

        datetimestring = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y%m%d%H")
        data = {'time': datetimestring}

        response = requests.post('http://127.0.0.1:8000/process/', data=data)
        # print (response.status_code, response.reason, response.request)
        print (response.text, response.json())

        if response.status_code == 200:
            r = response.json()
            self.slack.chat.post_message(self.channel, "Process" +"\n" + "URL : {}".format(r['url']) + "\n" "RECORDS : {}".format(r['length']) +"\n" "USERS : {}".format(r['users']))
            return response.request

        else:
            self.slack.chat.post_message(self.channel, " Process : preprocess fail.")
            return response.status_code

    def send_post_request_to_log(self):

        datetimestring = (datetime.utcnow() - timedelta(hours=1)).strftime("%Y%m%d%H")
        file_name = datetimestring + ".json"
        bucket = "temp/userlog_daily/"
        path = bucket + file_name
        data = {'path': path}

        response = requests.post('http://127.0.0.1:8000/log/', data=data)

        print (response.status_code, response.reason)

        if response.status_code == 200:
            self.slack.chat.post_message(self.channel, "Log : done.")
            return response.request
        else:
            self.slack.chat.post_message(self.channel, "Log : fail.")
            return response.status_code

    def send_post_request_to_aging(self):

        AGE_POINT = "30"
        GAP_RATE = "0.2"

        data = {'age_point': AGE_POINT, 'gap_rate': GAP_RATE}

        response = requests.post('http://127.0.0.1:8000/aging/', data=data)

        print (response.status_code, response.reason)

        if response.status_code == 200:
            self.slack.chat.post_message(self.channel, "Aging : finished.")
            return response.request
        else:
            self.slack.chat.post_message(self.channel, "Aging : failed.")
            return response.status_code
