from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG("query_db", 
          description="Query to database", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)

create_table = PostgresOperator(task_id='create_table', postgres_conn_id='postgres_conn', sql='CREATE TABLE IF NOT EXISTS teste(id int);', dag=dag)

insert_data = PostgresOperator(task_id='insert_data', postgres_conn_id='postgres_conn', sql='INSERT INTO teste VALUES (1);', dag=dag)

get_data = PostgresOperator(task_id='get_data', postgres_conn_id='postgres_conn', sql='SELECT * FROM teste;', dag=dag)

def print_info(ti):
  task_instance = ti.xcom_pull(task_ids='get_data')
  print("resultado da consulta: ---------")

  for row in task_instance:
    print(row)  

print_result = PythonOperator(task_id='print_result_task', python_callable=print_info, provide_context=True, dag=dag)





create_table >> insert_data >> get_data >> print_result