# -*- coding: utf-8 -*-
__author__ = 'robert'

# thread
import threading

# datetime, time
from datetime import datetime, timedelta
import time

# error log
import sys
import pytz
import traceback

# API controller
from API_controller import API_controller

# Slack
from slacker import Slacker


class DataMonitor(threading.Thread):


    # channel
    channel = '#data_crawler'
    before = ''
    check_time = 3600
    daily_batch_time = 18
    SLACK_TOKEN = 'YOUR_SLACK_TOKEN'

    def __init__(self, controller):

        threading.Thread.__init__(self)
        self.controller = controller

        self.slack = Slacker(SLACK_TOKEN)
        self.slack.chat.post_message(self.channel, self.channel + " start")

    def run(self):

        while(True):

            # get server datetime

            now = datetime.utcnow()

            # run class that send post request to preprocess and actionlog API
            try:
                self.controller.send_post_request_to_process()
                self.controller.send_post_request_to_log()

                if int(now.hour) == self.daily_batch_time:
                    # run class that send post request to clustering and aging API
                    self.controller.send_post_request_to_aging()

            except Exception as e:
                print('- API_trigger ' + str(datetime.now(tz=pytz.timezone('Asia/Seoul'))))
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                print(''.join('* ' + line for line in lines))

            time.sleep(self.check_time)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        pass


if __name__ == "__main__":

    controller = API_controller()

    monitor = DataMonitor(controller)

    monitor.start()
