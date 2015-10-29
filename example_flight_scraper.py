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
    name = "NK"
    lcc = True
    def str_to_float(self, float_str, decimal_point):
        no_cruft = re.sub(r"[^\d{0}]".format(decimal_point), '', float_str)
        parts = no_cruft.split(decimal_point)

        if len(parts) == 1:
            return float(parts[0])
        else:
            return float('{0}{1}{2}'.format(parts[0],
                                            locale.localeconv()['decimal_point'],
                                            parts[1]))

    def extract_price_and_currency(self, text):
        price = re.search(r"[.,\d]+", text).group(0)
        currency = text.replace(price, '').strip()
        currency = {u'$': 'USD'}[currency]
        price = self.str_to_float(price, '.')
        return price, currency

    def routes(self):
        routes = []

        self.g.go('http://www.spirit.com/Default.aspx')
        
        markets = re.search(r"var markets\s*=\s*({.+});", self.g.response.body).group(1)

        for dept, arrvs in json.loads(markets).items():
            for arrv in arrvs:
                self.response.append((dept, arrv))

        return routes

    def intial_search(self, fly_from, fly_to, date_from, passengers):
        self.g.setup(post={
            'bookingType': 'F',
            'tripType': 'oneWay',
            'from': fly_from,
            'to': fly_to,
            'departDate': date_from.strftime('%B %d, %Y'),
            'ADT': str(passengers),
            'CHD': '0',
            'INF': '0',
        })
        self.g.go('http://www.spirit.com/Default.aspx?action=search')

    def load_next_month(self, date):
        self.g.go('/DPPCalendarMarketAjax.aspx?market=1&action=next&month={0}&day={1}&year={2}'.format(
                  date.month, date.day, date.year))

    def load_date(self, date, market_index, date_from, date_to):
        self.g.go('/DPPMarketAjax.aspx?marketindex={0}&Month={1}&Day={2}&Year={3}'.format(
                  market_index, date.month, date.day, date.year))

    def input_wiht_lowest_fare(self, tr):
        inputs = tr.select(".//*[@type='radio'][not(contains(@class, 'memberFare'))]")
        elem = None
        min_price = 1000000

        for inp in inputs:
            price = re.search(r"[.,\d]+", inp.select("./following-sibling::em").text()).group(0)
            price = self.str_to_float(price, '.')
            if price <= min_price:
                min_price = price
                elem = inp

        return elem

    def flights(self, fly_from, fly_to, date_from, date_to, passengers):
        flights = []

        self.intial_search(fly_from, fly_to, date_from, passengers)
        first_day_of_month = date_from.replace(day=1)
        self.g.setup(headers={'X-Requested-With': 'XMLHttpRequest'})
        
        file_ = codecs.open("ls.html","w", "utf-8")
        file_.write(self.g.doc.select("/*").text())
        file_.close()

        while first_day_of_month <= date_to.replace(day=1):
            for href in self.g.doc.select("//a[contains(@href, 'selectionChanged')]").attr_list('href'):
                print href
                parts = re.findall(r"\'(.+?)\',", href)
                date = datetime.strptime('{0}-{1}-{2}'.format(parts[3], parts[1], parts[2]), '%Y-%m-%d')

                if date < date_from:
                    continue
                elif date > date_to:
                    return flights

                self.load_date(date, parts[0], date_from, date_to)

                for tr in self.g.doc.select("//tr[contains(@id, 'market1_trip_')]"):
                    try:
                        print "try"
                        if re.findall(r'[1-9] Stop', tr.select("//a[contains(@class, 'stopsLink')]").text().strip())[0]:
                            print "skip 1 stop"
                            continue
                    except:
                        pass
                    
                    inp = self.input_wiht_lowest_fare(tr)
                    if not inp:
                        continue

                    val = inp.attr('value')
                    if '^' in val:
                        continue  # connecting flight
                    elif self.name not in val:
                        continue  # operated by another airline
                    num = int(val.split("|")[1].split("~")[1])
                    m = re.findall(r"\d+\/\d+\/\d+ \d{2}:\d{2}", val)
                    departure = datetime.strptime(m[0], '%m/%d/%Y %H:%M')
                    arrival = datetime.strptime(m[1], '%m/%d/%Y %H:%M')

                    if departure.date() < date_from.date():
                        continue
                    elif departure.date() > date_to.date():
                        return flights

                    price, currency = self.extract_price_and_currency(inp.select("./following-sibling::em").text())
                    self.create_response(
                        airline = self.name,
                        src = fly_from,
                        dst = fly_to,
                        dtime = departure,
                        atime = arrival,
                        passengers = str(passengers),
                        currency = currency,
                        price = price * passengers,
                        number = str(num)
                    )

            self.load_next_month(first_day_of_month)
            first_day_of_month = (first_day_of_month + timedelta(days=45)).replace(day=1)

        return flights

if __name__ == '__main__':
    print "starting"
    engine().run()