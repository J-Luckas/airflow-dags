from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
import requests

dag = DAG("httpsensors", 
          description="http sensors", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)

def query_api():
  response = requests.get('https://api.publicapis.org/entries')
  print(f"{response.text}")

check_api = HttpSensor(task_id='check_api', http_conn_id='http_connection', endpoint='entries', poke_interval=5, timeout=20, dag=dag )
call_text_api = PythonOperator(task_id='call_text_api', python_callable=query_api, dag=dag)

check_api >> call_text_api