from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


dag = DAG("variaveis", 
          description="DAG Variaveis", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)


def print_var(**context):
  myVar = Variable.get('my_var')
  print(f"O valor da variável é: {myVar}")



task1 = PythonOperator(task_id="tsk1", python_callable=print_var , dag=dag)




task1

