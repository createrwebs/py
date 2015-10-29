from datetime import datetime,timedelta
from dateutil import rrule
from grab import Grab
import pycurl
import os
import re
import locale
os.environ['TZ'] = 'GMT'
import lxml.html
import ujson
import time
from time import sleep
import argparse
import csv
import codecs
import pytz
from pytz import country_timezones
import dateutil.parser
from elasticsearch import Elasticsearch
from grab.error import GrabNetworkError, GrabTimeoutError, GrabError
from operator import itemgetter
from fabric.api import *
from fabtools import require,utils,python,deb,cron
env.use_ssh_config = True
env.user = "root"
env.warn_only = True
env.timeout = 3600

import redis
import sys
import os
import shutil
sys.path.append("/srv/Scrapers")
sys.path.append("/srv/Scrapers/api")

from config import Config
import fabfile
from decorators import *
import certifi

sys.path.append("/srv/Scrapers/")
# from scraperlib.proxies.luminati import *
# from scraperlib.proxies.luminati import get_luminati_proxy
from statsd import *
# config_database = create_db_decorator(config.pg_config, connection = False)

# import random
# from failure_statistics import failure_statistics
# from redis_backend import redis_backend


class ScraperError(Exception):
	pass


class FlightsPlaceHolderNotFoundError(ScraperError):
	def __init__(self, msg=None):
		if not msg:
			msg = 'Placeholder for flights not found'
		super(FlightsPlaceHolderNotFoundError, self).__init__(msg)


class RouteNotFoundError(ScraperError):
	def __init__(self, route=None):
		if route:
			msg = 'Route {0}-{1} doesn\'t exist on site'.format(*route)
		else:
			msg = 'Specified route doesn\'t exist on site'
		super(RouteNotFoundError, self).__init__(msg)


class ProxyError(ScraperError):
	def __init__(self, proxy=None):
		super(ProxyError, self).__init__(proxy)


class MaintenanceError(ScraperError):
	def __init__(self, more_info=""):
		super(MaintenanceError, self).__init__(more_info)


class InternalServerError(ScraperError):
	def __init__(self, msg=''):
		super(InternalServerError, self).__init__(msg)


class FixMeError(ScraperError):
	def __init__(self, msg):
		super(FixMeError, self).__init__(msg)


class sGrab(Grab):
	total_upload_size = 0  # total size of uploaded data in bytes (incuding headers)
	total_download_size = 0  # total size of received data in bytes (incuding headers)

	def go(self, *args, **kwargs):
		super(sGrab, self).go(*args, **kwargs)
		#self.increment_traffic()
		if self.response.body == 'Proxy Error':
			raise ProxyError('Proxy Error')

	def submit(self, *args, **kwargs):
		super(sGrab, self).submit(*args, **kwargs)
		#self.increment_traffic()
		if self.response.body == 'Proxy Error':
			raise ProxyError('Proxy Error')

	def increment_traffic(self):
		try:
			self.total_upload_size += len(self.request_head)
			self.total_upload_size += len(self.request_body)
			self.total_download_size += len(self.response.head)
			if not self.config['nobody']:
				self.total_download_size += self.transport.curl.getinfo(pycurl.SIZE_DOWNLOAD)
		except:
			pass


# @config_database
# def update_proxy_health(cur, proxy, name):
# 	try:
# 		cur.execute("SELECT * FROM proxy_health WHERE ip = %s and airline = %s LIMIT 1", (proxy, name))
# 		data = cur.fetchone()
# 		if data:
# 			# sync server time and labs time
# 			database_now = datetime.now() + timedelta(hours=2)
# 			if database_now > data["next_try"]:
# 				if (database_now - data["next_try"]).seconds // 3600 < 2**int(data["blocked_requests"])+1: # if proxy was blocked earlier.. increase intervals
# 					cur.execute("""UPDATE
# 									 proxy_health
# 								   SET
# 									 last_block = NOW(),
# 									 blocked_requests = blocked_requests + 1,
# 									 next_try = %s
# 								   WHERE ip = %s and airline = %s""", (database_now + timedelta(hours=(2**(int(data["blocked_requests"]  +  1)))), proxy, name))
# 				else:
# 					cur.execute("""UPDATE
# 									 proxy_health
# 								   SET
# 									 last_block = NOW(),
# 									 next_try = %s
# 								   WHERE ip = %s and airline = %s""", (database_now + timedelta(hours=2**int(data["blocked_requests"])), proxy, name))
# 		elif proxy == None:
# 			pass
# 		elif "lum-customer-skypicker" not in proxy:
# 			cur.execute("INSERT INTO proxy_health (ip, airline, last_block, blocked_requests, blocking_threshold, next_try) VALUES (%s, %s, NOW(), 1, 1000000, NOW() + interval '2 hour')", (proxy, name))
# 	except Exception, e:
# 		print traceback.format_exc(e)

	
class scraper_model(object):
	lcc = False
	feed = False
	this_path = '/srv/Scrapers/all_scrapers/'
	rb_enabled = True  # enablig/diabling flag for redis backend

	def __init__(self,modul = False):
		self.modul = modul
		self.response = []
		self.proxy = ""
		self.g = sGrab()
		self.g.setup(user_agent_file=os.path.join(self.this_path, 'data', 'user_agents.txt'))
		self.g.setup(hammer_mode=True, hammer_timeouts=((20, 25), (20, 35)))
		self.g.transport.curl.setopt(pycurl.SSL_VERIFYPEER, 0)
		self.g.transport.curl.setopt(pycurl.SSL_VERIFYHOST, 0)
		self.id = 0
		self.is_return = False
		self.return_days = []
		self.timeout = None
		# self.failures = failure_statistics()
		# self.r = redis_backend(self.rb_enabled)

		if not modul:
			self.load_clasic()

	def load_clasic(self):
		self.parse_args()
		# print self.args
		self.process = "data"
		self.path = "/srv/results/"
		if self.args.proxy:
			self.proxy = self.args.proxy
			self.g.setup(proxy=self.args.proxy, proxy_type='http')
		if self.args.airlines:
			self.airlines = self.args.airlines.split(",")
		else:
			self.airlines = None
		if self.args.return_days:
			self.return_days = [int(x) for x in self.args.return_days.split(",")]
			self.is_return = True
		else:
			self.return_days = []
			self.is_return = False
		self.id = 0

	def run(self,src=None,dst=None,date_from=None,date_to=None,passengers=None,proxy=None,prev_proxy=None,return_days = None,force_super_proxy=False, segments=None,timeout = None):
		self.timeout = timeout
		if not self.modul:
			try:
				if self.args.routes:
					print "running routes"
					self.process = "routes"
					self.routes()
					metro_codes = ujson.load(open(os.path.join(self.this_path, 'data', 'metro_codes.json')))
					routes = []
					for route in self.response:
						for dept_airport in metro_codes.get(route[0], [route[0]]):
							for arrv_airport in metro_codes.get(route[1], [route[1]]):
								if (len(dept_airport.encode('ascii', 'ignore')) == 3 and
								    len(arrv_airport.encode('ascii', 'ignore')) == 3):
									routes.append((dept_airport, arrv_airport))
					self.response = sorted(list(set(routes)))
					self.save_csv()
				else:
					print "running data"
					self.process = "data"
					if self.args.verbose:
						log_path = '/tmp/scrapers_logs/{}'.format(self.name)
						if os.path.exists(log_path):
							shutil.rmtree(log_path)
						os.makedirs(log_path)
						self.g.setup(log_dir=log_path, debug=True)

					datefrom = datetime.strptime(self.args.datefrom, '%d-%m-%Y')
					dateto = datetime.strptime(self.args.dateto, '%d-%m-%Y')
					self.dates_to_scrape = list(rrule.rrule(rrule.DAILY, dtstart=datefrom, until=dateto))
					self.flights(self.args.flyfrom, self.args.flyto, datefrom, dateto, int(self.args.passengers))
					self.save_csv()
			except:
				exc_info = sys.exc_info()
				try:
					pass
					#sys.stderr.write(self.g.response.body)
				except:
					print "no response body"
				raise exc_info[1], None, exc_info[2]
		else:
			if return_days:
				self.return_days = [int(x) for x in return_days.split(",") if x != ""]
				self.is_return = True
			self.proxy = proxy
			self.segments = segments
			if proxy:
				self.g.setup(proxy=proxy, proxy_type='http')
			main_airlines = ["W6","U2","AK","DY","VY","5J","PC","4U","OD"]
			self.g.setup(proxy=proxy, proxy_type='http')
			results_ok = False
			elapsed_time = 0
			command = "--flyfrom %s --flyto %s --datefrom %s --dateto %s --passengers %s" % (src, dst, date_from.strftime("%d-%m-%Y"), date_to.strftime("%d-%m-%Y"), passengers)
			self.dates_to_scrape = list(rrule.rrule(rrule.DAILY, dtstart=date_from, until=date_to))
			try:
				start = time.time()
				pure_metro_codes = ["PAR","MIL","LON","REK","NYC","DTT","SAO","ROM","JKT","QMI","RIO","BUH","SPK","TCI","OSA","SEL","BUE","YMQ","CHI","STO","WAS","QDF","TYO","QSF","YTO","YEA","MOW","BJS","QHO","QLA"]
				if src not in pure_metro_codes and dst not in pure_metro_codes:
					self.flights(src, dst, date_from, date_to, passengers)
				results_ok = True #results are ok, we dont want to throw it out
				if prev_proxy is not None: #works fine without proxy or with luminati
					raise ProxyError #but we want to set proxy error

			except RouteNotFoundError, e: #plan task for update routes? Maybe routes function is not working properly.
				self.failures.route_not_found
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)

				self.monitor({
					"proxy": proxy,
					"airline": self.name,
					"error_type": "RouteNotFoundError",
					"@timestamp": datetime.now(),
					"command": command
				}, on=True)
			except GrabTimeoutError, e:
				self.failures.grab_timeout_error
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)
				elapsed_time = (time.time() - start)
				exc_info = sys.exc_info()
				if self.name in []:
					self.monitor({
					   "proxy": proxy,
					   "prev_proxy": prev_proxy,
					   "airline": self.name,
					   "error": str(e),
					   "elapsed_time":elapsed_time,
					   "@timestamp": datetime.now(),
					   "command": command
					}, on=True, service="elasticsearch")
				if prev_proxy is None and force_super_proxy: #(self.name not in main_airlines or force_super_proxy):
					lum_proxy = get_luminati_proxy(zone=self.name)
					self.run(src, dst, date_from, date_to, passengers, lum_proxy, proxy) #try with luminati proxy but only for test airlines
				else:
					raise exc_info[1], None, exc_info[2]
			except GrabNetworkError, e: #maybe block
				self.failures.grab_network_error
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)
				exc_info = sys.exc_info()
				self.monitor({
					"proxy": proxy,
					"airline":self.name,
					"e": str(e),
					"error_type":"GrabNetworkError",
					"@timestamp": datetime.now(),
					"command": command
				}, on=True)
				if prev_proxy is None and force_super_proxy: #(self.name not in main_airlines or force_super_proxy):
					lum_proxy = get_luminati_proxy(zone=self.name)
					self.run(src, dst, date_from, date_to, passengers, lum_proxy, proxy) #try with luminati proxy but only for test airlines
				else:
					raise exc_info[1], None, exc_info[2]
			except GrabError as e:
				exc_info = sys.exc_info()
				getattr(self.failures, exc_info[0].__name__.lower())
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)
				raise exc_info[1], None, exc_info[2]
			except FlightsPlaceHolderNotFoundError, e: #maybe block
				self.failures.flights_placeholder_not_found
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)
				exc_info = sys.exc_info()
				self.monitor({
					"proxy": proxy,
					"airline":self.name,
					"error_type":"FlightsPlaceHolderNotFoundError",
					"@timestamp": datetime.now(),
					"command": command
				}, on=True)
				if prev_proxy is None and force_super_proxy: #(self.name not in main_airlines or force_super_proxy):
					lum_proxy = get_luminati_proxy(zone=self.name)
					self.run(src, dst, date_from, date_to, passengers, lum_proxy, proxy) #try with luminati proxy
				else:
					raise exc_info[1], None, exc_info[2]
			except ProxyError, e: #100 percent block
				self.failures.proxy_error
				exc_info = sys.exc_info()
				if prev_proxy:
					proxy = prev_proxy #blocked proxy
				update_proxy_health(proxy, self.name)
				if results_ok is False:
					raise exc_info[1], None, exc_info[2]
			except MaintenanceError as e:
				self.failures.maintenance_error
				exc_info = sys.exc_info()
				raise exc_info[1], None, exc_info[2]
			except InternalServerError as e:
				self.failures.internal_server_error
				exc_info = sys.exc_info()
				raise exc_info[1], None, exc_info[2]
			except FixMeError as e:
				self.failures.fix_me_error
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)
				exc_info = sys.exc_info()
				raise exc_info[1], None, exc_info[2]
			except ScraperError as e:
				exc_info = sys.exc_info()
				getattr(self.failures, exc_info[0].__name__.lower())
				raise exc_info[1], None, exc_info[2]
			except Exception, e: #maybe block
				exc_info = sys.exc_info()

				#if prev_proxy is None and self.name not in main_airlines:
				#	self.run(src, dst, date_from, date_to, passengers, None, proxy) #try without proxy #TODO None is not solution :/
				#else:

				getattr(self.failures, exc_info[0].__name__.lower())
				self.r.dump_failed_run(type(e).__name__, self.name, src, dst, date_from, date_to, passengers, self.g)

				"""
				self.monitor({}, on=True, service="datadog")
				self.monitor({
					"proxy": proxy,
					"prev_proxy": prev_proxy,
					"airline": self.name,
					"error_type": "GeneralException",
					"error": str(e),
					"traceback": str(traceback.format_exc(e)),
					"@timestamp": datetime.now(),
					"command": command
				}, on=True, service="elasticsearch")
				"""
				raise exc_info[1], None, exc_info[2]
			finally:
				self.r.upload_failure_statistics(self.name, self.failures)

			try:
				if self.g.response:
					r = redis.Redis(unix_socket_path='/tmp/redis.sock',password = config.redis_password)

					traf = r.get('%s_traffic_down'%self.name)
					r.set('%s_traffic_down'%self.name, float(traf if traf else 0.0) + self.g.response.download_size)
					traf = r.get('%s_traffic_up'%self.name)
					r.set('%s_traffic_up'%self.name, float(traf if traf else 0.0) + self.g.response.upload_size)
					traf = r.get('%s_requests'%self.name)
					r.set('%s_requests'%self.name, int(traf if traf else 0) + 1)
			except Exception, e:
				print traceback.format_exc(e)
			elapsed_time = (time.time() - start)
			if self.name in []:
				self.monitor({
						"proxy": proxy,
						"prev_proxy": prev_proxy,
						"airline": self.name,
						"elapsed_time":elapsed_time,
						"@timestamp": datetime.now(),
						"command": command
				}, on=True, service="elasticsearch")

			for flight in self.response:
				if re.match('\d+-\d+-\d+ \d+:\d+:\d+', flight[3]):  # oneway flight
					dept_time = flight[3]
				else:  # return flight
					dept_time = flight[5]
				if not (date_from.date() <= datetime.strptime(dept_time, '%Y-%m-%d %H:%M:%S').date() <= date_to.date()):
					break

			return self.response

	def monitor(self, data, on=False, service="datadog"):
		if on:
			if service == "elasticsearch":
				try:
					es = Elasticsearch(
						config.es["nodes"],
						port=9000,
						http_auth=(config.es["auth"]["user"], "X"),
						verify_certs=False,
					)
					es.create(index="skypicker", doc_type="scrape_task", body=data, ignore=409, timeout=5)
				except Exception, e:
					print e


	def next_flight(self):
		self.id +=1

	def create_response_with_id(self,airline,src,dst,dtime,atime,passengers,currency,price,number = None,extras = "",return_flight = 0):
		if number:
			builded = [str(self.id),str(return_flight),airline,src,dst,dtime.strftime('%Y-%m-%d %H:%M:%S'),atime.strftime('%Y-%m-%d %H:%M:%S'),currency,str(price),extras,str(passengers),datetime.now().strftime('%Y-%m-%d %H:%M:%S'),number]
		else:
			builded = [str(self.id),str(return_flight),airline,src,dst,dtime.strftime('%Y-%m-%d %H:%M:%S'),atime.strftime('%Y-%m-%d %H:%M:%S'),currency,str(price),extras,str(passengers),datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
		if not self.feed:
			print ";".join(builded)
		self.response.append(builded)
		return builded

	def create_response(self,airline,src,dst,dtime,atime,passengers,currency,price,number = None,extras = ""):
		if number == None:
			builded = [airline,src,dst,dtime.strftime('%Y-%m-%d %H:%M:%S'),atime.strftime('%Y-%m-%d %H:%M:%S'),currency,str(price),extras,str(passengers),datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
		else:
			builded = [airline,src,dst,dtime.strftime('%Y-%m-%d %H:%M:%S'),atime.strftime('%Y-%m-%d %H:%M:%S'),currency,str(price),extras,str(passengers),datetime.now().strftime('%Y-%m-%d %H:%M:%S'),str(number)]
		if not self.feed:
			print ";".join(builded)
		self.response.append(builded)
		return builded

	def get_csv_name(self):
		if self.process == "routes":
			return self.name+"_"+"routes.csv"
		else:
			ret_string = ""
			if self.is_return:
				ret = [str(x) for x in self.return_days]
				ret_string = "_".join(ret)
			if self.lcc:
				return "_".join([self.name,self.args.flyfrom,self.args.flyto,self.args.datefrom,self.args.dateto,str(self.args.passengers)])+".csv"
			if self.is_return:
				return "_".join([self.name,self.args.flyfrom,self.args.flyto,self.args.datefrom,self.args.dateto,str(self.args.passengers),ret_string])+".csv"
			else:
				return "_".join([self.name,self.args.flyfrom,self.args.flyto,self.args.datefrom,self.args.dateto,str(self.args.passengers)])+".csv"
	def save_csv(self):
		print self.get_csv_name()
		f = csv.writer(codecs.open(self.path+self.get_csv_name(),"w", "utf-8"), delimiter=';',quoting=csv.QUOTE_ALL)
		for row in self.response:
			print row
			f.writerow(row)

	def save_file(self, filename = "test.html",path = "/srv/Scrapers/all_scrapers/html/",body = ""):
		"""Save html page to file, with timestamp in filename"""
		tz = pytz.timezone(country_timezones("CZ")[0])
		final_name = "%s%s_%s_%s" % (path, filename, datetime.now(tz).strftime("%H-%M_%d-%m-%Y"), random.random())
		final_name = final_name.replace(".html","")
		final_name += ".html"
		print "saving page as %s ..." % final_name
		with open(final_name,"wb+") as f:
			f.write(body)

	def parse_args(self):
		parser = argparse.ArgumentParser()
		parser.add_argument('--routes', action='store_true')
		parser.add_argument('--flyfrom', type=str)
		parser.add_argument('--flyto', type=str)
		parser.add_argument('--datefrom', type=str)
		parser.add_argument('--dateto', type=str)
		parser.add_argument('--passengers', type=int)
		parser.add_argument('--proxy', type=str,default = None)
		parser.add_argument('--airlines', type=str,default = None)
		parser.add_argument('--return_days', type=str,default = None)
		parser.add_argument('--verbose', action='store_true')
		self.args = parser.parse_args()
	def parse_price(self, string, decimal_mark=None):
		if type(string) in [int, float]:
			return float(string)

		if decimal_mark:
			no_cruft = re.sub(r"[^\d{0}]".format(decimal_mark), '', string)
			parts = no_cruft.split(decimal_mark)
			if len(parts) == 1:
				return float(parts[0])
			else:
				return float('{0}{1}{2}'.format(parts[0], locale.localeconv()['decimal_point'], parts[1]))

		prices = re.findall(r"([\d+]+)",string)
		float_places = 0
		full_numbers = 0
		if len(prices) == 1:
			float_places = float(prices[0])
		elif len(prices) > 1:
			if len(prices[-1]) != 3:
				float_places = float("0."+prices[-1])
				full_numbers = int("".join(prices[0:-1]))
			else:
				full_numbers = int("".join(prices))
		return float(float_places) + float(full_numbers)

	def parse_fare_key(self, fare_key, date_format=None):
		"""
		Extract flight info from so called 'market fare key'.
		Example: 0~B~~B00PMN5~IF5N~~2~X|F9~  98~ ~~DEN~04/04/2015 12:20~CUN~04/04/2015 17:05~
		Return airline code, flight number, departure airport, departure time,
		arrival airport, arrival time
		"""
		rex = re.compile('\|(\w+)~\s*(\w+)~.+?([A-Z]{3})~([\d/]+\s*[\d:]+)~([A-Z]{3})~([\d/]+\s*[\d:]+)')
		res = rex.search(fare_key)
		if not res:
			# swap left and right part from |
			fare_key = '|'.join(reversed(fare_key.split('|')))
			res = rex.search(fare_key)
		airline, flight_number, src, departure, dst, arrival = res.groups()
		if date_format:
			departure, arrival = map(lambda x: datetime.strptime(x, date_format), [departure, arrival])
		return airline, flight_number, src, departure, dst, arrival

	def build_flight_times(self, dept_date, dept_time, arrv_time, time_format=None):
		""" Build complete departure/arrival times from short time strings and departure date. """
		if type(dept_time) in [str, unicode] and type(arrv_time) in [str, unicode]:
			if time_format:
				dept_time, arrv_time = map(lambda x: datetime.strptime(x, time_format), [dept_time, arrv_time])
			else:
				dept_time, arrv_time = map(lambda x: dateutil.parser.parse(x, fuzzy=True), [dept_time, arrv_time])
		return map(lambda x: datetime.combine(dept_date.date(), x.time()), [dept_time, arrv_time])

	def lowest_fare(self, items, fare_val_func, result_val_func,
			exclude_fares=[], exclude_val_func=None, decimal_mark=None):
		""" Find lowest fare.

		We can't rely on assumption that lowest fare for a flight on
		particular site is always first one or last, or whatever.
		We should always look through all available fares.

		Args:
			items            -- any iterable object of fare items
			fare_val_func	 -- function to extract numeric fare representation from item.
					    This is what's compared.
			result_val_func  -- function to extract a value from item.
					    This is what's returned.
			exclude_fares	 -- list of fares we don't want
			exclude_val_func -- function to extract value that is tested for
					    membership in exclude_fares list.
			decimal_mark	 -- decimal_mark for parse_price

		Returns:
			Fare value or some value. In second case it's usually some sort
			item's id that's used for further receiving of final price.

		Usage example:
			fare_id = self.lowest_fare(tr.select(".//select/option"),
						   lambda x: x.attr('value').split('|')[1],
						   lambda x: x.attr('value'),
						   ['Premium Members Only'],
						   lambda x: x.attr('value').split('|')[2],
						   ',')
		"""
		fares = [(self.parse_price(fare_val_func(item), decimal_mark), result_val_func(item))
			 for item in items if not exclude_fares or exclude_val_func(item) not in exclude_fares]
		if fares:
			return sorted(fares, key=itemgetter(0))[0][1]
		else:
			return None

	def extract_price_and_currency(self, text, decimal_mark=None):
		""" Extract price (numeric part) and currency (the rest) from a text. """
		price = re.search('[.,\d]+', text).group(0)
		currency = re.sub(price, '', text)
		currency = re.sub('\s+', '', currency)
		return self.parse_price(price, decimal_mark), currency

	def set_dryscrape_session_proxy(self, session):
		if self.g.config['proxy']:
			proxy = re.search('(\d+\.\d+\.\d+\.\d+:\d+)', self.g.config['proxy']).group(1)
			if 'http://' in self.g.config['proxy']: # luminati
				session.set_proxy(
						host=proxy.split(':')[0],
						port=int(proxy.split(':')[1]),
						user=re.findall("(lum.+?)-session", self.g.config['proxy'])[0],
						password='a0e180a352ae')
			else:
				session.set_proxy(host=proxy.split(':')[0], port=int(proxy.split(':')[1]))