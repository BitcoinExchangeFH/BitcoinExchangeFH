#!/bin/python

try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import json


class RESTfulApi:
    """
    Generic REST API call
    """
    def __init__(self):
        """
        Constructor
        """
        pass

    def request(self, url):
        """
        Web request
        :param: url: The url link
        :return JSON object
        """
        res = urlrequest.urlopen(url)
        res = json.loads(res.read().decode('utf8'))
        return res


