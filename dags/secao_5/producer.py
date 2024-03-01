from airflow import DAG, Dataset
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd


dag = DAG("producer", 
          description="producer", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)

mydataset = Dataset('/opt/airflow/data/Churn-new.csv')

def get_file_from_dataset():
  dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
  dataset.to_csv('/opt/airflow/data/Churn-new.csv', sep=';')


setNewFile = PythonOperator(task_id='setFile', python_callable=get_file_from_dataset, dag=dag, outlets=[mydataset])

setNewFile