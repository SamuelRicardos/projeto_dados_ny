import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
import requests
import json

dag = DAG(
    'tutorial_dag_2',
    start_date=datetime(2021, 12, 1),
    schedule_interval='30 * * * *',
    catchup=False
)

def captura_conta_dados(**kwargs):
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    kwargs['ti'].xcom_push(key='qtd', value=qtd)


captura_conta_dados = PythonOperator(
    task_id='captura_conta_dados',
    python_callable=captura_conta_dados,
    provide_context=True,
    dag=dag
)

def e_valida(**kwargs):
    ti = kwargs['ti']
    qtd = ti.xcom_pull(task_ids='captura_conta_dados', key='qtd')
    
    if qtd > 1000:
        return 'valido'
    return 'nvalido'


e_valida = BranchPythonOperator(
    task_id='e_valida',
    python_callable=e_valida,
    provide_context=True,
    dag=dag
)

valido = BashOperator(
    task_id='valido',
    bash_command="echo 'Quantidade OK'",
    dag=dag
)

nvalido = BashOperator(
    task_id='nvalido',
    bash_command="echo 'Quantidade não OK'",
    dag=dag
)

captura_conta_dados >> e_valida >> [valido, nvalido]
