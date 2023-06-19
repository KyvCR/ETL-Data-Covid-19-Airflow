from datetime import datetime, timedelta
from airflow import DAG


from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator


default_args = {
    'owner': 'Keyvi',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 2),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'catchup': False
}

dag = DAG(
    dag_id='db_mysql',
    default_args=default_args,
    schedule_interval='* */30 * * *'
)


query = 'SELECT country,total_pet_store FROM pet_stores'
task_1 = MySqlOperator(
    task_id='run_query',
    mysql_conn_id='Dibimbing_DB',
    sql=query,
    dag=dag
)

print(task_1)
