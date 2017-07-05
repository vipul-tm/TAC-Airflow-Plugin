# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.hooks.base_hook import BaseHook
import paramiko

class SSHParmikoHook(BaseHook):
    """
    Interact with SSH via paramiko.
    """

    conn_name_attr = 'paramiko_conn_id'
    default_conn_name = 'telrad_default'
   

    def __init__(self, ssh_conn_id = default_conn_name):
        #super(RedisHook, self).__init__(*args, **kwargs)
        #self.schema = kwargs.pop("schema", None)
        self.ssh_conn_id = ssh_conn_id
        self.conn = self.get_connection(ssh_conn_id)

    def get_client(self):
        """
        Returns a redis connection object
        """
        conn_config = {
            "host": self.conn.host or 'localhost',
            "port": self.conn.port or 22,
        "login": self.conn.login or 'admin',
        "password":self.conn.password or 'admin'
        }

    
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(conn_config['host'],username=conn_config['login'],password=conn_config['password'])
        return client

    def close(self,client):
        client.close()

    def getShell(self,client):
        shell = client.invoke_shell()
        return shell


