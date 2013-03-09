#!/usr/bin/env python
# encoding: utf-8
"""
basestream.py

Created by Brian Eoff on 2011-02-21.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import pycurl
import os
import signal
from urllib import urlencode


class Stream:

    def __init__(self, url, username, password, on_receive_method, initial_params=[], filter_type=None):
        self.url, self.username, self.password = url, username, password
        self.on_receive, self.childPid = on_receive_method, -1
        self.params = initial_params
        self.filter_type = filter_type
        self.conn = pycurl.Curl()

    def resetFilterParameters(self, params):
        self.params = params
        self.restart()

    def restart(self):
        self.stop()
        self.start()

    def stop(self):
        os.kill(self.childPid, signal.SIGTERM)
        os.waitpid(self.childPid, 0)

    def start(self):
        self.conn.setopt(pycurl.VERBOSE ,1)
        self.conn.setopt(pycurl.HTTPAUTH, pycurl.HTTPAUTH_BASIC)
        self.conn.setopt(pycurl.USERPWD, "%s:%s" % 
                          (self.username, self.password))
        self.conn.setopt(pycurl.URL, self.url)
        self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
        if len(self.params) > 0:
            self.conn.setopt(pycurl.POST, 1)
            self.conn.setopt(pycurl.POSTFIELDS, 
                           urlencode({self.filter_type: ','.join(self.params)}))
            
        cpid = os.fork()
        if cpid == 0:
            try:
                self.conn.perform()
            except:
                print 'Unable to start Curl'
                pass
        else:
            self.childPid = cpid


if __name__ == '__main__':
    pass	
