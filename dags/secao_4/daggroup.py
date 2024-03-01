from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup


dag = DAG("dag_group", 
          description="DAG Group", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False,
          default_view='graph'
)

task1 = BashOperator(task_id="tsk1", bash_command="sleep 5", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 5", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 5", dag=dag)
task4 = BashOperator(task_id="tsk4", bash_command="sleep 5", dag=dag)
task5 = BashOperator(task_id="tsk5", bash_command="sleep 5", dag=dag)
task6 = BashOperator(task_id="tsk6", bash_command="sleep 5", dag=dag)

taskGroup = TaskGroup("tsk_group1", dag=dag)

task7 = BashOperator(task_id="tsk7", bash_command="sleep 5", dag=dag, task_group=taskGroup)
task8 = BashOperator(task_id="tsk8", bash_command="sleep 5", dag=dag, task_group=taskGroup)
task9 = BashOperator(task_id="tsk9", bash_command="sleep 5", dag=dag,
                     trigger_rule="one_failed", task_group=taskGroup)


task1 >> task2
task3 >> task4

[task2, task4] >> task5

task5 >> task6

task6 >> taskGroup

