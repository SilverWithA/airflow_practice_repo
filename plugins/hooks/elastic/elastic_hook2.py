from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

from elasticsearch import Elasticsearch

class ElasticHook(BaseHook):
    def __init__(self, conn_id='elastic_default', *args, **kwargs):
        super().__init__(*args, **kwargs)   # 초기화
        conn = self.get_connection(conn_id) # 연결 설정

        conn_config = {}
        hosts = []

        if conn.host:
             hosts = conn.hooks.split(',')

        if conn.port:
            conn_config['port'] = int(conn.port)

        if conn.login:
            conn_config['http_auth'] = (conn.login, conn.password)

        # hook 초기화
        self.es = Elasticsearch(hosts, **conn_config) # conn_config = 연결 설정을 위한 모든 파라미터(호스트, 포트, 로그인 etc)
        
        # index 설정: airflow에서 connections에서 설정해준 schema를 참고
        self.index = conn.schema
    

    # Elasticsearch와 소통하기 위한 매서드 정의 ---------------------------------------------
    def info(self):
        return self.es.info()
    
    def set_index(self,index):
        self.index = index
    
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, doc=doc)
        return res

# AirflowPlugin manager에 ElasticHook가 hook이라는 걸 알려줘야함
class AirflowElasticPlugin(AirflowPlugin):
    name = 'elastic'
    hooks = [ElasticHook]


        