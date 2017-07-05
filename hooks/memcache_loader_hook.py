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

import memcache
from airflow.hooks.base_hook import BaseHook

class MemcacheHook(BaseHook):
    """
    Interact with Memcache.
    """
    conn_name_attr = 'memc_cnx_id'
    default_conn_name = 'memc_default'
    supports_autocommit = True

    def __init__(self, memc_cnx_id = 'memcache'):
        self.memc_cnx_id = memc_cnx_id
        self.cnx = self.get_connection(memc_cnx_id)

    def get_cnx(self):
        if self.cnx.host and self.cnx.port:
            cnx_string = str(self.cnx.host)+":"+str(self.cnx.port)
        else:
            cnx_string = '10.133.19.165:11211'

        MEMCACHE_CONFIG = [cnx_string]
        #MEMCACHE_CONFIG = ['10.133.19.165:11211','10.133.12.163:11211']
        memc_cnx= memcache.Client(
                            MEMCACHE_CONFIG, debug=1,server_max_value_length = 1024*1024*10
                            )
        return memc_cnx

    def set(self,key,value):
        conn = self.get_cnx()
        conn.set(key,value)

    def get(self,key):
        conn = self.get_cnx()
        return conn.get(key)



