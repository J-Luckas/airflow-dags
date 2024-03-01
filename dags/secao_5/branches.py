from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import random


dag = DAG("branch", 
          description="branch test", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)


def getRandomNumber():
  return random.randint(1, 100)


def defineRandomNumber( **context ):
  number = context['task_instance'].xcom_pull(task_ids='random_number')

  if number % 2 == 0:
    return 'par_task'
  else:
    return 'impar_task'
    



get_random_number_task = PythonOperator(task_id='random_number', python_callable=getRandomNumber, dag=dag)
define_random_number_task = BranchPythonOperator(task_id='define_random_number', python_callable=defineRandomNumber, provide_context=True, dag=dag)
par_task = BashOperator(task_id='par_task', bash_command='echo "Número par!"', dag=dag)
impar_task = BashOperator(task_id='impar_task', bash_command='echo "Número impar!"', dag=dag)


get_random_number_task >> define_random_number_task

define_random_number_task >> par_task
define_random_number_task >> impar_task






