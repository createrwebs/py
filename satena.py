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
    name = "9R"

    def routes(self):
        routes = []

        self.g.config['user_agent'] = 'Mozilla/5.0'
        self.g.go('https://secure.kiusys.net/satena-ibe/index.php')
        dest = []

        for loc in self.g.doc.select("//select[@name='origen']/option[contains(@class, 'origen_')]"):
            dest.append(loc.text()[-3:])

        for x in range(len(dest)):
            for y in range(x+1, len(dest)):
                self.response.append((dest[x],dest[y]))
                self.response.append((dest[y],dest[x]))
        return

    def intial_search(self, fly_from, fly_to, date_from, passengers):
        self.g.setup(post={
            'trayecto': 'ida',
            'origenesIata': fly_from,
            'destinosIata': fly_to,
            'fdesde': date_from.strftime('%d/%m/%Y'),
            'fhasta': '',
            'mayores': str(passengers),
            'consultar': '1',
        })
        self.g.config['user_agent'] = 'Mozilla/5.0'
        self.g.go('https://secure.kiusys.net/satena-ibe/resultados.php')

        self.g.setup(post={
            'tipoViaje': 'ida',
            'accion': 'getRespuesta',
        })
        self.g.go('https://secure.kiusys.net/satena-ibe/resultados.php')

    def load_date(self, date, fly_from, fly_to):
        self.g.setup(post={
            'datos': '{}//{}/{}/1/0/0'.format(date.strftime('%Y-%m-%d'), fly_from, fly_to),
            'tipoViaje': 'ida',
            'accion': 'nuevoDia',
        })
        self.g.go('/satena-ibe/resultados.php')

    def flights(self, fly_from, fly_to, date_from, date_to, passengers):
        flights = []

        self.intial_search(fly_from, fly_to, date_from, passengers)

        actualDate = date_from

        while actualDate.date() <= date_to.date():
            for row in self.g.doc.select("//tr[@class='tr_segmentos']"):
                # eliminace letu s prestupem
                height = row.select("td[contains(@id, 'tede')]").attr('style')[-4:]
                if (height != "80px"):
                    continue

                minPrice = float("inf")
                for element in row.select("td[contains(@id, 'tede')]"):
                    price = float(element.select("label").text())
                    if (minPrice > price):
                        minPrice = price

                value = row.select("td[contains(@id, 'tede')]/input").attr('value')
                departure = dateutil.parser.parse(re.search(r'fecha=([\d|-]* [\d|:]*)#', value).group(1))
                arrival = dateutil.parser.parse(re.search(r'fechaLegada=([\d|-]* [\d|:]*)#', value).group(1))

                self.create_response(
                    airline = self.name,
                    src = fly_from,
                    dst = fly_to,
                    dtime = departure,
                    atime = arrival,
                    passengers = str(passengers),
                    currency = 'COP',
                    price = minPrice * passengers,
                )
            actualDate += timedelta(days=1)
            self.load_date(actualDate, fly_from, fly_to)
        return flights

if __name__ == '__main__':
    print "starting"
    engine().run()