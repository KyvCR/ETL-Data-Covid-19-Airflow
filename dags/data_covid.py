from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.edgemodifier import Label

import requests as rs
import pandas as pd
import json


default_args = {
    'owner': 'Keyvi',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'catchup': False
}

dag = DAG(
    dag_id='data_covid',
    default_args=default_args,
    schedule_interval='0 4 * * *'
)


# extract data from API
url = ('https://api.covid19api.com/summary')
response = rs.get(url).json()["Countries"]

# transform Data
# define function get data from API dengan kondisi country = Indonesia


def country():
    result = []
    for x in response:
        if x['Country'] in "Indonesia":
            result.append(x)
    return result

#
# celaning data jika key(premium) = {}


def cleaningData():
    data = country()
    result = []
    for val in data:
        if val['Premium'] == {}:
            val['Premium'] = 0
    result.append(val)
    return result


def CrateDataFrame():
    result = cleaningData()
    result_pd = pd.DataFrame(result)
    return result_pd


# load Data
# query create new table
query_create = '''
        CREATE TABLE covid_data_KeyviCR(
            ID SERIAL PRIMARY KEY,
            Country VARCHAR(50),
            CountryCode VARCHAR(5),
            Slug VARCHAR(50),
            NewConfirmed INT,
            TotalConfirmed INT,
            NewDeaths INT,
            TotalDeaths INT,
            NewRecovered INT,
            TotalRecovered INT,
            Date DATETIME,
            Premium INT)

'''

# membuat tabel baru


def CreateTabel():
    # try:
    df = CrateDataFrame()
    table_name = 'covid_data_KeyviCR'
    mysql_hook = MySqlHook(mysql_conn_id='Dibimbing_DB')
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query_create)
    cursor.close()
    conn.commit()

    df.to_sql(table_name, mysql_hook.get_sqlalchemy_engine(),
              index=False)
    # except:
    #     print('Table covid_data_KeyviCR already exists')


# mengambil value dari fuction cleaning data
value_data = []
for i in cleaningData():
    for x in i.values():
        value_data.append(x)

# query untuk insert data
query_insert = '''
        INSERT INTO covid_data_KeyviCR VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''


# Task DAGS
with dag:
    task_1 = PythonOperator(
        task_id='Get_API',
        python_callable=country,
    )

    task_2 = PythonOperator(
        task_id='Cleaning_Data',
        python_callable=cleaningData,
    )

    task_3 = PythonOperator(
        task_id='Create_New_DataFrame_from_API',
        python_callable=CrateDataFrame,
    )

    task_4 = PythonOperator(
        task_id='Create_tabel',
        python_callable=CreateTabel,
    )

    task_5 = MySqlOperator(
        task_id='insert_data_to_mysql',
        mysql_conn_id='Dibimbing_DB',
        sql=query_insert,
        parameters=value_data,
        trigger_rule='one_failed'
    )


# Urutan Task
task_1 >> task_2 >> task_3 >> Label(
    'Create New Tabel') >> task_4 >> Label('Insert Data') >> task_5
# note task_4 akan running sukses jika belum ada tabel, jika table sudah ada makan akan error, kemudian task_5(insert data) akan tetap jalan melalui task_3, kondisinya dapat dari trigeer_rule
task_3 >> Label('Lansung Insert Data') >> task_5
