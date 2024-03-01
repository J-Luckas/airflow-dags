from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator


dag = DAG("dag_run_xcom", 
          description="DAG run xcom", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)

def task_write( **kwarg ): 
  kwarg['ti'].xcom_push(key='valorXcom1', value=10200)

task1 = PythonOperator(task_id="tsk1", python_callable=task_write , dag=dag)


def task_read( **kwarg ):
  valor = kwarg['ti'].xcom_pull(key='valorXcom1')
  print(f"valor recuperado: {valor}")

task2 = PythonOperator(task_id="tsk2", python_callable=task_read, dag=dag)

task1 >> task2

