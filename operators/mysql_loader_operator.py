import logging
import tempfile
import csv
import os
import traceback 
import time 
from airflow.hooks import MemcacheHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import MySqlHook
from airflow.hooks import RedisHook

class MySqlLoaderOperator(BaseOperator):
	"""
	transfers memc or redis data to specific mysql table

	:param mysql_conn_id: reference to a specific mysql database
	:type mysql_conn_id: string
	:param sql: the sql code to be executed
	:type sql: Can receive a str representing a sql statement,
		a list of str (sql statements), or reference to a template file.
		Template reference are recognized by str ending in '.sql'
	"""

	template_fields = ('sql',)
	template_ext = ('.sql',)
	ui_color = '#edffed'
	arguments= "self, sql,memc_key,mysql_table,exec_type='bulk',update=False,memc_conn_id = 'memc_cnx', mysql_conn_id='mysql_uat', parameters=None,	autocommit=True,db_coloumns=None,input_type='memc'"

	@apply_defaults
	def __init__(
			self, sql,memc_key,mysql_table,exec_type="bulk",update=False,memc_conn_id = 'memc_cnx', mysql_conn_id='mysql_uat', parameters=None,
			autocommit=True,db_coloumns=None,input_type="memc", *args, **kwargs):
		super(MemcToMySqlOperator, self).__init__(*args, **kwargs)
		self.mysql_conn_id = mysql_conn_id
		self.sql = sql
		self.memc_key = memc_key
		self.mysql_table = mysql_table
		self.memc_conn_id = memc_conn_id
		self.exec_type = exec_type
		self.autocommit = autocommit
		self.parameters = parameters
		self.memc_conn_id = memc_conn_id
		self.update = update
		self.db_coloumns = db_coloumns
		self.db = "_".join(memc_key.split("_")[2:4])
		self.input_type = input_type
	def execute(self, context):
		start_main = time.time()
		logging.info('Executing: ' + str(self.sql))
		logging.info("memcache key:%s"%self.memc_key)
		hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
		
		
		try:
			if input_type == "memc":
				memc_hook = MemcacheHook(memc_cnx_id = self.memc_conn_id)
				data = memc_hook.get(self.memc_key)
				keys = data[0].keys()
			elif input_type == "redis":
				redis_hook = RedisHook(redis_conn_id=aelf.memc_conn_id)
				data =eval(redis_hook.rget(self.memc_key))
				if data != None and len(redis_data) > 0:
					keys = eval(data[0]).keys()
				else:
					logging.info("Unable to extract dict keys")

		except Exception:
			logging.info("Not able to get the data from %s"%(self.input_type))
			traceback.print_exc()
			
		if len(redis_data) > 0:
			if self.exec_type == "bulk":	
				with tempfile.NamedTemporaryFile(delete=False) as temp:
					dict_writer = csv.DictWriter(temp,keys,lineterminator=";\n",delimiter="|",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
					for slot_data in redis_data:
						try:
							dict_writer.writerows(slot_data)
						except UnicodeEncodeError:
							logging.info("Got Unicode charater")
					
					temp.close()
					start_file = time.time()	
					if self.update:
						query = "LOAD DATA LOCAL INFILE \'%s\' REPLACE INTO TABLE %s.%s FIELDS TERMINATED BY '|' LINES TERMINATED BY ';' (%s);" %(temp.name,self.db,self.mysql_table,self.db_coloumns)
					else:
						query = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE %s.%s FIELDS TERMINATED BY '|' LINES TERMINATED BY ';' (%s);" %(temp.name,self.db,self.mysql_table,self.db_coloumns)
					hook.run(
						query,
						autocommit=self.autocommit,
						parameters=self.parameters)
				os.unlink(temp.name)	
				
			else:
				if not self.update:
					
					conn = hook.get_conn()
					cursor = conn.cursor()
					logging.info("We are about to upload %s data in total ."%len(data))		
					try:
						cursor.executemany(self.sql, new_data)
						logging.info("Successfully executed Query")
					except Exception:
						logging.info("Exception")
						traceback.print_exc()
					conn.commit()
					cursor.close()
					conn.close()

				else:
					conn = hook.get_conn()
					cursor = conn.cursor()
					logging.info("We are about to update %s data in total ."%len(new_data))
					try:
						cursor.executemany(self.sql,new_data)
						logging.info("Successfully executed Query")
					except Exception:
						logging.info("Exception")
						traceback.print_exc()
					
					conn.commit()
					cursor.close()
					conn.close()
				
		else:
			logging.info("No Data recieved from memc or redis") 
