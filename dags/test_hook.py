from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook


http_hook = HttpHook(
    method='GET', http_conn_id='https://api.covid19api.com/summary')

default_args = {
    'owner': 'Keyvi',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 2),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'catchup': False
}

dag = DAG(
    dag_id='Test_Hook',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)


def download_data():
    response = http_hook.run(endpoint='get')
    data = response.json()


download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag
)

download_data_task
