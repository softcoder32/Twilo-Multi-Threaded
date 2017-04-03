__author__ = 'djff'

import os
import csv
import queue
import logging
import threading
from datetime import datetime
from optparse import OptionParser
from twilio import TwilioRestException
from twilio.rest import TwilioRestClient


class BulkSMS:
    def __init__(self, accnt_sid, auth_token):
        self.message = None
        self.que = queue.Queue()
        self.client = TwilioRestClient(accnt_sid, auth_token)
        self.lock = threading.Lock()

    def set_creative(self, filename):
        """Method used to parse creative file
            and set self.message variable
        """
        fp = self.get_fp(filename)
        self.message = fp.read()

    def send_message(self, phone):
        """Methos used to send messges given
            sender's number and recepient.
        """
        sender = '+19382385793'
        try:
            message = self.client.messages.create(to=phone, from_=sender, body=self.message)
            with self.lock:
                self.success.info("[*] Message successfully sent to Recepient {} at {}".format(phone, datetime.now()))
        except TwilioRestException as e:
            with self.lock:
                self.failure.info("[*] Message Failed to send to Recepient {}".format(phone))

    def worker(self):
        """method used to collect threads
            in the queue for execution
        """
        while True:
            phone = self.que.get()
            self.send_message(phone)
            self.que.task_done()

    def set_threads(self, num):
        """Method's used to load threads
            in a queue
        """
        for tid in range(num):
            thread = threading.Thread(target=self.worker)
            thread.daemon = True
            thread.start()

    def set_loggers(self):
        """Used to create logger
            files
        """
        self.setup_logger('success', r'successfully-sent.log')
        self.setup_logger('failure', r'failure-sending.log')
        self.setup_logger('general', r'general-activity.log')
        self.success = logging.getLogger('success')
        self.failure = logging.getLogger('failure')
        self.general = logging.getLogger('general')

    @staticmethod
    def setup_logger(logger_name, log_file, level=logging.INFO):
        """Used to configure logger files
        """
        l = logging.getLogger(logger_name)
        formatter = logging.Formatter('INFO : %(message)s')
        file_handler = logging.FileHandler(log_file, mode='a')
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(file_handler)
        l.addHandler(stream_handler)

    @staticmethod
    def get_fp(filename):
        """create a file pointer
            give the filename
        """
        return open(filename, 'r')

if __name__ == '__main__':
    account_ssid = 'ACbde02aa4ed8238f58b9e553ffb5439fb'
    auth_token = '5a2ae04b3054ede79306726d9c4545ad'

    sms = BulkSMS(account_ssid, auth_token)

    parser = OptionParser()
    parser.add_option('--subs', action='store', dest='subscriber', default=None)
    parser.add_option('--unsubs', action='store', dest='unsubscribe', default=None)
    parser.add_option('--msg', action='store', dest='text_msg', default=None)
    parser.add_option('--threads', action='store', dest='threads', default=50)

    options, rem = parser.parse_args()

    reader = csv.reader(sms.get_fp(options.subscriber))
    black = csv.reader(sms.get_fp(options.unsubscribe))

    sms.set_creative(options.text_msg)
    sms.set_threads(int(options.threads))

    directory = os.getcwd() + '/logs'
    if not os.path.exists(directory):
        os.makedirs(directory)
    os.chdir(directory)

    sms.set_loggers()
    black_list = []

    for num in black:
        black_list.append(num[0])

    for row in reader:
        try:
            if row[0] in black_list:
                sms.general.error("[*] Tried sending to Unsubscribed Recipient. Skipping {}".format(row[0]))
                continue
            sms.que.put(row[0])
        except:
            pass
    sms.que.join()
    print("Complete!!")