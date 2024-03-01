from airflow import DAG, Dataset
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd


consumerDataset = Dataset('/opt/airflow/data/Churn-new.csv')
dag = DAG("consumer", 
          description="consumer", 
          schedule=[consumerDataset], 
          start_date=datetime(2024,2,20), 
          catchup=False
)

def get_info_from_dataset():
  dataset = pd.read_csv('/opt/airflow/data/Churn-new.csv', sep=';')
  dataset.to_csv('/opt/airflow/data/Churn-new2.csv', sep=';')


setNewFile = PythonOperator(task_id='setFile', python_callable=get_info_from_dataset, dag=dag, provide_context=True)

setNewFile