from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
import csv
import requests
import json

# task에 대한 공통 사항
default_args = {
    "owner":"airflow",
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"psych5268@naver.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}


# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"

    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}

            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]

            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

with DAG("forex_pipeline2", start_date = datetime(2023,1,1)
         ,schedule_interval="@daily", default_args = default_args,
         catchup=False) as dag:
    
    #### HttpSensor ####
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        http_conn_id="forex_api",  # airflow connection에서 설정한 api id 
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b", # url의 host뒤에 오는 요소
        response_check=lambda response: "rates" in response.text, # api 가용성 체크 기준
        poke_interval=5,
        timeout=20
    )

    #### FileSensor ####
    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id = "forex_path",  # connection에서 설정한 path 정보
        filepath="forex_currencies.csv", # 존재 유무 체크할 파일 이름
        poke_interval=5,
        timeout=20
    )
    #### PythonOperator ####
    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=download_rates
    )

    #### BashOperator ####
    saving_rates = BashOperator(
        task_id = "saving_rates",

        # hdfs forex라는 폴더 생성
        # 생성된 폴더로 파일 put

        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )


    #### HiveOperator ####
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
    ### SparkSubmitOperator ###
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )


    is_forex_rates_available >> is_forex_currencies_file_available >> downloading_rates >> saving_rates 
    saving_rates >> creating_forex_rates_table >> forex_processing

    
