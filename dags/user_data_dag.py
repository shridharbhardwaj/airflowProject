from datetime import *
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from random_user_api import run_userdata_etl

default_args = {
    'owner': 'shree',
    'depends_on_past':False,
    'start_date':datetime(2024,2,26),
    'email':['bhardwaj@gmail.com'],
    'emial_on_failure' : True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

dag=DAG(
    'user_data_dag',
    default_args=default_args,
    description='Random User Details from across the globe'
)

run_etl = PythonOperator(
    task_id='Complete_user_etl',
    python_callable=run_userdata_etl,
    dag=dag
)

run_etl







