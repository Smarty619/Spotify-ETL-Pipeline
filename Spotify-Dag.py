from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from Spotify_ETL import etl

default_args = {
    'owner': 'airflow_dat',
    'depends_on_past': False,
    'start_date': datetime(2023, 08, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='First DAG with ETL process!',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='Complete_spotify_etl',
    python_callable=etl,
    dag=dag, 
)

run_etl
