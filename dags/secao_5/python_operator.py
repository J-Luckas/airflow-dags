from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import statistics as sts


dag = DAG("pythonoperator", 
          description="python operator", 
          schedule_interval=None, 
          start_date=datetime(2024,2,20), 
          catchup=False
)

def data_cleaner():

  dataset= pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')

  dataset.columns = [ 'Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio', 'Saldo', 'Produtos', 'TemCartCredito', 'Ativo', 'Salario', 'Saiu' ]

  mediana = sts.median(dataset['Salario'])

  dataset['Salario'].fillna(mediana, inplace=True)

  dataset['Genero'].fillna('Masculino', inplace=True)

  mediana = sts.median(dataset['Idade'])

  dataset.loc[(dataset['Idade'] < 0) | (dataset['Idade'] > 120), 'Idade' ] = mediana

  dataset.drop_duplicates(subset='Id', keep='first', inplace=True)

  dataset.to_csv('/opt/airflow/data/Clean-Churn.csv', sep=';', index=False)



data_cleaner_task = PythonOperator(task_id='data_cleaner_task', python_callable=data_cleaner, dag=dag)


data_cleaner