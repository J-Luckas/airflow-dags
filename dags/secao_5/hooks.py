from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG("hook", 
          description="Query to database by hook", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)

def create_table():
  sql = 'CREATE TABLE IF NOT EXISTS teste2(id int);'
  pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
  pg_hook.run(sql, autocommit=True)

def insert_table():
  sql = 'INSERT INTO teste2 VALUES (1);'
  pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
  pg_hook.run(sql, autocommit=True)

def select_table(**kwargs):
  sql = 'SELECT * FROM teste2;'
  pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
  records = pg_hook.get_records(sql)

  kwargs['ti'].xcom_push(key='query_result', value=records)

def print_data(ti):
  task_instance = ti.xcom_pull(key='query_result', task_ids='select_table_task')
  print('Dados: -------')

  for row in task_instance:
    print(row[0])

create_table_task = PythonOperator(task_id='create_table_task', python_callable=create_table, dag=dag)
insert_table_task = PythonOperator( task_id='insert_table_task', python_callable=insert_table, dag=dag)
select_table_task = PythonOperator(task_id='select_table_task', python_callable=select_table, provide_context=True, dag=dag)
print_data_task = PythonOperator(task_id='print_data_task', python_callable=print_data, provide_context=True, dag=dag)

create_table_task >> insert_table_task >> select_table_task >> print_data_task
