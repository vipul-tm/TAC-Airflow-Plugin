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
class TelradExtractor(BaseOperator):
	"""
	This is a telrad operator which executes the provided query on the given connection name and then extracts the output and stores in 
	redis with timestamp and Data

	Requirement :SSHParmiko

	Connection:The Telrad connection should have:
		1) Conn Id
		2) Host
		3) Login - username
		4) Password: 
		5) Port
		6) Extra: Su username and pass

		"""

	
	ui_color = '#edffed'
	arguments= "self,query,telrad_conn_id,redis_conn_id,wait_str, *args, **kwargs"

	@apply_defaults
	def __init__(
			self,query,telrad_conn_id,redis_conn_id,wait_str, *args, **kwargs):
		super(TelradExtractor, self).__init__(*args, **kwargs)
		self.query=query
		self.telrad_conn_id = telrad_conn_id
		self.redis_conn_id = redis_conn_id
		self.wait_string = wait_str

	def send_string_and_wait(command, wait_time, should_print,shell):
		# Send the su command
		shell.send(command)
		# Wait a bit, if necessary
		time.sleep(wait_time)
		# Flush the receive buffer
		receive_buffer = shell.recv(1024)

	def send_string_and_wait_for_string(command, wait_string, should_print,shell):
		# Send the su command
		shell.send(command)
		time.sleep(.5)
		value = []
		# Create a new receive buffer
		receive_buffer = ""
		while not wait_string in receive_buffer:#TODO: As deque is good for pop and push ,thinking to replace the DS to deques but list are good for slicing so will do openerations first on list then change DS
			# Flush the receive buf
			#receive_buffer += shell.recv(1024)
			receive_buffer = shell.recv(1024)
			#time.sleep(.5)
			value.append(receive_buffer)
			shell.send("\n")	 #/n is required to get data from the BV server	
		return value


	def execute(self, context):
		logging.info("Executing Telrad Extractor Operator")
		start_date= context.get("execution_date")
		hook = SSHParmikoHook()	
		x = hook.get_client()
		shell = hook.getShell(x)
		logging.info("Successfully created shell fort BV")
		send_string_and_wait("sudo su  lteadmin \n", 1, True,shell)
		# Send the client's su password followed by a newline
		send_string_and_wait(system_su_password + "\n", 1, True,shell)	
		send_string_and_wait("ncs_cli -u admin \n", 1, True,shell)
		value_raw = send_string_and_wait_for_string(self.query+ "\n", self.wait_string, True,shell)
		#TODO: Add to redis with timestamp for this data