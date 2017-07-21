#!/usr/bin/python3

import sqlite3
import configparser
import urllib.request
import time

OLD_TIME = "1970-01-01 00:00:00"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

class Record():
	def __init__(self, name, sampling_period, sampling_method):
		self.name = name
		self.sampling_period = sampling_period
		self.sampling_method = sampling_method

	def get_info(self):
		print("name: %s, sampling period is %s, samping method is %s"
			% (self.name, self.sampling_period, self.sampling_method))

def read_config(config_file):
	config = configparser.ConfigParser()
	config.read(config_file)
	config_dict = {}
	config_dict['db_path'] = config['db']['db_path']
	config_dict['aa_mgmt_url'] = config['aa']['aa_mgmt_url']
	return config_dict

def connect_db(db_path):
	conn = sqlite3.connect(db_path)
	return conn

def get_db_update_time(conn):
	update_timestamp = time.mktime(time.strptime(OLD_TIME,TIME_FORMAT))
	SQL = ''' SELECT time
	          FROM update_time
	          ORDER BY time DESC
	          LIMIT 1
	      '''
	cursor = conn.execute(SQL)

	for row in cursor:
		time_str = row[0]
		update_timestamp = time.mktime(time.strptime(time_str,TIME_FORMAT))

	return update_timestamp

def get_record_list(conn):
	record_list = []
	SQL = ''' SELECT rname, value
          FROM record_name, recinfo
          WHERE record_name.rec = recinfo.rec AND key = 'arch'
      '''
	cursor = conn.execute(SQL)
	
	for row in cursor:
		name = row[0]
		config = row[1].split(',')
		if not config[0] == '0':
			sampling_period = config[1]
			sampling_method = config[2]
			record_tmp = Record(name, sampling_period, sampling_method)
			record_list.append(record_tmp)
			record_tmp.get_info()
	return record_list

def archiver_record(record, mgmt_url):
	url_archiver = mgmt_url + '/archivePV'
	print("process archiver")
	url = url_archiver + '?pv=' + record.name + \
						'&samplingperiod=' + record.sampling_period + \
						'&samplingmethod=' + record.sampling_method
	urllib.request.urlopen(url)
	print(url)

def _main():

	config_dict = read_config('autoarchiver_config.ini')
	print(config_dict)
	db_conn = connect_db(config_dict['db_path'])

	last_time = time.mktime(time.strptime(OLD_TIME,TIME_FORMAT))
	while True:
		time.sleep(1)
		update_time = get_db_update_time(db_conn)
		if update_time <= last_time:
			continue
		else:
			record_list = get_record_list(db_conn)
			for record in record_list:
				archiver_record(record, config_dict['aa_mgmt_url'])
			last_time = update_time

if __name__ == "__main__":
	_main()
	
