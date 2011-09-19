#!/usr/bin/env python
# encoding: utf-8
"""
stream_functions.py

Created by Brian Eoff on 2011-02-21.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import logging
import simplejson as json


class Buffer:

    buffer_text = ''

    def __init__(self):
        pass

    def append(self, buffer_str):
        self.__class__.buffer_text = self.__class__.buffer_text + buffer_str

    def get(self):
        return self.__class__.buffer_text

    def empty(self):
        self.__class__.buffer_text = ''


class OutputFileHandler:

    file_handle = None

    def __init__(self):
        pass

    def set(self, f_handle):
        self.__class__.file_handle = f_handle

    def write(self, content):
        self.__class__.file_handle.write(content)

    def close(self):
        self.__class__.file_handle.close()

    def flush(self):
        self.__class__.file_handle.flush()


def on_receive(data):
    """
    The generic on_receive function. Writes each
    tweet to file
    """

    fl_output = OutputFileHandler()
    buffer = Buffer()
    buffer.append(data)

    if data.endswith('\r\n'):
        output_txt = buffer.get().strip()
        if len(output_txt) > 0:
            try:
                #json.loads(output_text)
                fl_output.write(output_txt)
                fl_output.write('\n')
                fl_output.flush()
            except:
                logging.warning('Unable to parse JSON: ' + output_txt + '\n')

        buffer.empty()


def date_to_fname_string(date):
    """
    Takes a datetime object and returns a string.
    Used for creating the file name
    """

    if date.minute > 9:
        dateStr = str(date.day) + "_" + str(date.month) + "_" + \
                  str(date.year) + "_" + str(date.hour) + ":" + \
                  str(date.minute)
    else:
        dateStr = str(date.day) + "_" + str(date.month) + "_" + \
                  str(date.year) + "_" + str(date.hour) + ":0" + \
                  str(date.minute)

    return dateStr
