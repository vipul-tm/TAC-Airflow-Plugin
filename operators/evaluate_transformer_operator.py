import logging

import traceback 
import time 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import RedisHook

class EvaluateTransformer(BaseOperator):
	"""
	This Operator is used to evaluate string data for furthur processing ,it is python eval function<br />

	<b> Requirement </b> :- <br />

	Connection:The Telrad connection should have: <br />
		1) redis_conn : the Redis Hook Object <br />
		2) identifier_input <br />
		3) identifier_output <br />
		4) output_identifier_index <br />
		5) start_timestamp <br />
		6) end_timestamp <br />
		7) indexed <br />
	Timestamp Must be present in the 
		"""

	
	ui_color = '#edffed'
	arguments= """
		1) redis_conn : the Redis Hook Object <br />
		2) identifier_input <br />
		3) identifier_output <br />
		4) output_identifier_index <br />
		5) start_timestamp <br />
		6) end_timestamp <br />
		7) indexed <br />

		"""

	@apply_defaults
	def __init__(
			self,redis_conn,identifier_input,identifier_output,output_identifier_index,start_timestamp,end_timestamp,payload,index_key,indexed=False, *args, **kwargs):
		super(TelradExtractor, self).__init__(*args, **kwargs)
		self.redis_conn=redis_conn
		self.identifier_input = identifier_input
		self.start_timestamp = start_timestamp
		self.end_timestamp = end_timestamp
		self.identifier_output = identifier_output
		self.output_identifier_index = output_identifier_index
		self.payload = payload
		self.index_key = index_key
		self.indexed = indexed

	def execute(self, context):
		logging.info("Executing Evaluator Operator")
		if self.indexed:
			data = redis_conn.get_event_by_key(self.identifier_input,self.payload,self.index_key,self.start_timestamp,self.end_timestamp)
		else:
			data = redis_conn.get_event(self.identifier_input,self.start_timestamp,self.end_timestamp)

		evaluated_data =  eval(data)
		redis_conn.add_event_by_key(self.identifier_output,evaluated_data,{self.output_identifier_index:self.output_identifier_index})
