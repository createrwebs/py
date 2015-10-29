#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''
__author__ = 'Sergey Petrov'
__email__ = 'felyxjet@gmail.com'
'''
import logging
from time import sleep
import argparse
from datetime import datetime, timedelta
import json
import os
import re
import locale
import pycurl
from grab import Grab
from scraper_model import scraper_model
import pycurl
import os
import re
os.environ['TZ'] = 'GMT'
import lxml.html
import ujson
from time import sleep
import argparse
import csv
import codecs
import json
import dateutil.parser
from dateutil import rrule
import urllib
import ast
import locale
import xml.etree.ElementTree as ET
from pprint import pprint
import codecs

class except_error_code(Exception):
    message = "code is not 200"
class engine(scraper_model):
    name = "DP"

    def routes(self):
        routes = []

        self.g.go('https://booking.pobeda.aero/')
        
        markets = re.search(r"\"markets\": ({.*}]})", self.g.response.body).group(1)
        
        for dept, arrvs in (json.loads(markets)).items():
            for arrv in arrvs:
                self.response.append((dept, arrv['TravelLocationCode']))
        return routes

    def intial_search(self, fly_from, fly_to, date_from, passengers):
        url = urllib.urlencode({
            'marketType': 'oneWay',
            'fromStation': fly_from,
            'toStation': fly_to,
            'beginDate': date_from.strftime('%d-%m-%Y'),
            'endDate': (date_from + timedelta(days=1)).strftime('%d-%m-%Y'),
            'adultCount': str(passengers),
            'currencyCode': 'RUB',
            'utm_source': 'pobeda'
        })
        self.g.go('https://booking.pobeda.aero/ExternalSearch.aspx?%s' % url)

    def load_date(self, date):
        self.g.setup(post={
            'indexTrip': '1',
            'dateSelected': date.strftime('%Y-%m-%d')
        })
        self.g.go('/AjaxTripAvailaibility.aspx')

    def flights(self, fly_from, fly_to, date_from, date_to, passengers):
        flights = []

        self.intial_search(fly_from, fly_to, date_from, passengers)
        actualDate = date_from

        while actualDate.date() <= date_to.date():
            for row in self.g.doc.select('//div[contains(@class, "FareRow ")]'):
                departure = dateutil.parser.parse(row.attr('data-departuretime'))
                arrival = dateutil.parser.parse(row.attr('data-arrivaltime'))

                # pokud bude datum odletu pozdeji nez pozaduji, koncim
                if departure.date() < date_from.date():
                    continue
                elif departure.date() > date_to.date():
                    return flights


                if actualDate.date() == departure.date():
                    price = int("".join(row.text().split(" ")[-2:])[:-1])
                    # num = 'nondef'
                    self.create_response(
                        airline = self.name,
                        src = fly_from,
                        dst = fly_to,
                        dtime = departure,
                        atime = arrival,
                        passengers = str(passengers),
                        currency = 'RUB',
                        price = price * passengers,
                        # number = str(num)
                    )
    
            actualDate += timedelta(days=1)
            self.load_date(actualDate)

        return flights

if __name__ == '__main__':
    print "starting"
    engine().run()