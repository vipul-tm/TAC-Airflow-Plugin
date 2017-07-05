# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint
from flask_admin import BaseView, expose
from flask_admin.base import MenuLink

# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.models import XCom
from airflow.models import Variable
from airflow.hooks import RedisHook
import json
# Will show up under airflow.hooks.test_plugin.PluginHook
class PluginHook(BaseHook):
    pass

# Will show up under airflow.operators.test_plugin.PluginOperator
class PluginOperator(BaseOperator):
    pass

# Will show up under airflow.executors.test_plugin.PluginExecutor
class PluginExecutor(BaseExecutor):
    pass

# Will show up under airflow.macros.test_plugin.plugin_macro
def plugin_macro():
    pass

# Creating a flask admin BaseView
class TestView(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
       
        xlist = eval(Variable.get('rules'))
        xlist = json.dumps(xlist)
        attributes = []

        #for device_key in xlist:
        #    attributes.append((device_key, xlist.get(device_key)))

        
        return self.render("rules_plugin/rules.html",attributes=attributes,data=xlist)


# Creating a flask admin BaseView
class ServiceView(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
        redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
        sv_keys = redis_hook_4.get_keys("sv_agg_nocout_*")
        data_to_page = []
        attributes = []
        for key in sv_keys:
            data = redis_hook_4.rget(key)   
            for slot in data:
                slot = eval(slot)
                for k,v in enumerate(slot):
                    device = eval(v)
                    data_to_page.append(device)

        
        #for device_key in xlist:
        #    attributes.append((device_key, xlist.get(device_key)))

        
        return self.render("rules_plugin/rules.html",attributes=attributes,data=data_to_page)
# Creating a flask admin BaseView

class NetworkView(BaseView):
    @expose('/')
    def test(self):
        # in this example, put your test_plugin/test.html template at airflow/plugins/templates/test_plugin/test.htm
        redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
        nw_keys = redis_hook_4.get_keys("nw_agg_nocout_*")
        data_to_page = []
        attributes = []

        for key in nw_keys:
            data = redis_hook_4.rget(key)   
            for slot in data:
                slot = eval(slot)
                for k,v in enumerate(slot):
                    device = eval(v)
                    data_to_page.append(device)
        
           
        return self.render("rules_plugin/rules.html",attributes=attributes,data=data_to_page)

v = TestView(category="Rule Plugin", name="Rules View")
v2 = NetworkView(category="Rule Plugin", name="Network")
v3 = ServiceView(category="Rule Plugin", name="Service")

# Creating a flask blueprint to intergrate the templates and static folder
bp = Blueprint(
    "rule_plugin", __name__,
    template_folder='templates', # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/rules')


ml = MenuLink(
    category='Wirelessone',
    name='UAT',
    url='http://10.133.12.163')
ml2 = MenuLink(
    category='Wirelessone',
    name='Production',
    url='http://121.244.255.107')
# Defining the plugin class
class AirflowTestPlugin(AirflowPlugin):
    name = "rule_plugin"
    operators = [PluginOperator]
    hooks = [PluginHook]
    executors = [PluginExecutor]
    macros = [plugin_macro]
    admin_views = [v,v2,v3]
    flask_blueprints = [bp]
    menu_links = [ml,ml2]