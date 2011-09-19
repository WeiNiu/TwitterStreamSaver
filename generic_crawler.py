#!/usr/bin/env python
# encoding: utf-8
"""
generic_crawler.py

Created by Brian Eoff on 2011-02-21.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys
import os
import codecs
import time
import logging
import traceback
import simplejson as json
from datetime import datetime
from basestream import Stream
from ConfigParser import SafeConfigParser
from stream_functions import OutputFileHandler
from stream_functions import on_receive, date_to_fname_string

if __name__ == '__main__':

    """
    This type of crawler will be a generic. It does not need to interact
    with Twitter beyond being connected to the stream.
    """

    config_file = sys.argv[1]
    crawler_config = sys.argv[2]

    parser = SafeConfigParser()
    parser.read(config_file)

    dataDirectory = parser.get(crawler_config, 'directory')
    
    if not (os.path.exists(dataDirectory)):
        os.mkdir(dataDirectory)
    
    time_per_file = parser.getint(crawler_config, 'time_length')
    username = parser.get(crawler_config, 'username')
    password = parser.get(crawler_config, 'password')
    crawler_type = parser.get(crawler_config, 'type')
    crawler_id = parser.get(crawler_config, 'id')

    logging.basicConfig(filename = crawler_id + '.log', level = logging.ERROR)

    stream_url = parser.get(crawler_config, 'stream_url')

    trackTerms = []

    if parser.has_option(crawler_config, 'terms_file'):
        terms_file = open(parser.get(crawler_config, 'terms_file'), 'r')
        json_string = terms_file.read()
        trackTerms = json.loads(json_string)['terms']
        terms_file.close()

    while True:

        currentDate = datetime.now()
        dateStr = date_to_fname_string(currentDate)
        output = codecs.open(dataDirectory + '/' + dateStr + '-Tweets.txt', 
                              encoding='utf-8', mode='w+')
        ofh = OutputFileHandler()
        ofh.set(output)

        try:
            stream  = Stream(stream_url, username, password, on_receive, 
                              initial_params = trackTerms, filter_type = crawler_type)
            stream.start()
            time.sleep(time_per_file)	
            stream.stop()
            ofh.close()
        except Exception, err:
            logging.error(str(datetime.now()) + ':' + str(err))
            try:
                stream.stop()
                ofh.close()
            except Exception, err:
                logging.error(str(datetime.now()) + ':' + str(err))
                logging.warning('Unable to close stream/file.')
