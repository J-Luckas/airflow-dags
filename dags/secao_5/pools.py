from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator


dag = DAG("pool", 
          description="pool", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)



task1 = BashOperator(task_id="tsk1", bash_command='sleep 5' , dag=dag, pool='my_pool')
task2 = BashOperator(task_id="tsk2", bash_command='sleep 5' , dag=dag, pool='my_pool', priority_weight=5)
task3 = BashOperator(task_id="tsk3", bash_command='sleep 5' , dag=dag, pool='my_pool')
task4 = BashOperator(task_id="tsk4", bash_command='sleep 5' , dag=dag, pool='my_pool', priority_weight=10)






