from datetime import timedelta,datetime
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from twitter_etl import twitter_etl


default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2022,12,8),
}

dag=DAG(
    'twitter_dag',
    default_args=default_args,
    description='my first etl code'
)

run_etl=PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=twitter_etl,
    dag=dag
)

run_etl
