from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import timedelta, date
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covid-stock-pipeline',
    default_args=default_args,
    description='Covd Case Stock Market Data Pipeline',
    schedule_interval='0 18 * * 1-5',
    start_date=days_ago(1)
)

hook_extract = SSHHook(ssh_conn_id='ssh_extract')

task_extract = SSHOperator(
    task_id='extraction',
    command="python3 driver.py",
    ssh_hook=hook_extract,
    dag=dag
)

hook_clean = SSHHook(ssh_conn_id='ssh_clean')

task_clean = SSHOperator(
    task_id='clean',
    command="python3 driver-clean.py",
    ssh_hook=hook_clean,
    dag=dag
)

task_extract >> task_clean