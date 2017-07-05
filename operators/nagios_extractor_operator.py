import logging
import tempfile
import csv
import os
import traceback 
import time 
from airflow.hooks import MemcacheHook
from airflow.models import BaseOperator
from hooks.sshparamiko_extractor_hook import SSHParmikoHook
from airflow.utils.decorators import apply_defaults
from airflow.hooks import RedisHook
import socket
import paramiko
import sys
import re
import random

class Nagiosxtractor(BaseOperator):
	"""
	This is a Nagios operator which executes the provided query on the given connection name and then extracts the output and stores in 
	redis with timestamp and Data<br />

	<b>Requirement</b> :Nagios Connections and Query Connection <br /> 

	The Telrad connection should have: <br />
		1) query: The Query to be executeed on the socket<br />
		2) conn Id: The connection id specidied in airflow <br />
		3) redis_conn_id: redis connection id string<br />
		4) site_name: Naios Site name<br />
		5) site_ip: Nagios Site IP<br />
		6) site_port:Nagios Site  Port<br />
		7) identifier: The identifier from which data is to be accessed by other operators <br />

		"""

	
	ui_color = '#edffed'
	arguments= """
	The Telrad connection should have: <br />
		1) query<br />
		2) conn Id: <br />
		3) redis_conn_id: <br />
		4) site_name: <br />
		5) site_ip:<br />
		6) site_port<br />
		7) identifier<br />

		
	"""

	@apply_defaults
	def __init__(
			self,query,conn_id,redis_conn_id,site_name,site_ip,site_port,identifier, *args, **kwargs):
		super(TelradExtractor, self).__init__(*args, **kwargs)
		self.query=query
		self.telrad_conn_id = conn_id
		self.redis_conn_id = redis_conn_id
		self.site_name=site_name
		self.site_ip = site_ip
		self.site_port =  site_port
		self.device_slot = device_slot
	def get_from_socket(site_name,query,socket_ip,socket_port):
		"""
		Function_name : get_from_socket (collect the query data from the socket)

		Args: site_name (poller on which monitoring data is to be collected)

		Kwargs: query (query for which data to be collectes from nagios.)

		Return : None

		raise
		     Exception: SyntaxError,socket error
	    	"""
		#socket_path = "/omd/sites/%s/tmp/run/live" % site_name
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
		machine = site_name[:-8]
		s.connect((socket_ip, socket_port))
		#s.connect(socket_path)
		s.send(query)
		s.shutdown(socket.SHUT_WR)
		output = ''
		wait_string= ''
		while True:
			try:
				out = s.recv(100000000)
		     	except socket.timeout,e:
				err=e.args[0]
				print 'socket timeout ..Exiting'
				if err == 'timed out':
					sys.exit(1) 
		
		     	if not len(out):
				break;
		     	output += out

		return output
	
	def extract(**kwargs):	

		task_site = self.site_name
		site_name = self.site_name
		site_ip = self.site_ip
		site_port = self.site_port
		logging.info("Extracting data for site"+str(site_name)+"@"+str(site_ip)+" port "+str(site_port))
		data = []
		data = get_from_socket(site_name, self.query,site_ip,site_port)
		return data



	def execute(self, context):
		data = self.extract()
		timestamp= int(time.time())
		payload_dict = {
		"DAG_name":context.get("task_instance_key_str").split('_')[0],
		"task_name":context.get("task_instance_key_str"),
		"payload":data,
		"timestamp":timestamp
		}
		redis = RedisHook(redis_conn_id = "redis")
		conn = redis.get_conn()
		redis.add_event(identifier,timestamp,payload_dict)
