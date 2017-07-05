# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin
import copy
from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink
from operators.mysql_loader_operator  import MySqlLoaderOperator
from operators.telrad_extractor_operator import TelradExtractor
from operators.nagios_extractor_operator import Nagiosxtractor
from operators.evaluate_transformer_operator import EvaluateTransformer
from operators.list2dict_transformer_operator import List2DictTransformer
# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.models import XCom
from airflow.models import Variable
from hooks.redis_loader_hook import RedisHook
from hooks.memcache_loader_hook import MemcacheHook
import inspect

import json
# Creating a flask admin BaseView
OPERATORS = [MySqlLoaderOperator,TelradExtractor,Nagiosxtractor,EvaluateTransformer]
HOOKS = [RedisHook,MemcacheHook]
EXECUTOR = []
MACRO = []
ALL = []
ALL.extend(OPERATORS)
ALL.extend(HOOKS)
ALL.extend(EXECUTOR)
ALL.extend(MACRO)
############################################################################################################################################

class TacView(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
        attributes = []
        data_table = []
        operator_data={}

        for classes in ALL:
            operator_data['name']=str(classes.__name__)
            operator_data['type']=str(inspect.getfile(classes).split("_")[-1].split(".")[0])
            operator_data['module']=str(inspect.getfile(classes).split("_")[-2])
            try:
                operator_data['args']=str(classes.arguments) if classes.arguments else "NA"
            except Exception:
                operator_data['args']= "NOT FOUND"
            
            operator_data['desc']=str(inspect.getdoc(classes))
            operator_data['loc']=str(inspect.getfile(classes))          
            data_table.append(copy.deepcopy(operator_data))       
        
        #data_table = json.dumps(data_table)
        return self.render("tac_plugin/tac.html",attributes=attributes,data_table=data_table)

class intro(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
        attributes = []
        data_table = []
        operator_data={}
        intro_rules= eval(Variable.get("introrules"))
        for data in intro_rules:
            operator_data['rule']=str(data.get('rule'))
            operator_data['syntax']=str(data.get('syntax'))
            operator_data['example']=str(data.get('example'))
            operator_data['mandatory']=str(data.get('mandatory'))
            operator_data['desc']=str(data.get('desc'))          
            data_table.append(copy.deepcopy(operator_data))       
        
        #data_table = json.dumps(data_table)
        return self.render("tac_plugin/intro.html",attributes=attributes,data_table=data_table)




v = TacView(category="TAC Plugin", name="Operators")
introView = intro(category="TAC Plugin", name="Intro")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "tac_plugin", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/tac')


VIEWS = [v,introView]
BLUEPRINT = [bp]

# Defining the plugin class
class AirflowTacPlugin(AirflowPlugin):
    name = "tac_plugin"
    operators = OPERATORS
    hooks = HOOKS
    executors = EXECUTOR
    macros = MACRO
    admin_views = VIEWS
    flask_blueprints = BLUEPRINT
    #menu_links = [ml,ml2]