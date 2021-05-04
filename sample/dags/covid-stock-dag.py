from airflow.models import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from datetime import timedelta, date
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'covid-stock-pipeline',
    default_args=default_args,
    description='Covd Case Stock Market Data Pipeline',
    schedule_interval='@weekly',
    start_date=days_ago(1)
)

hook_extract = SSHHook(ssh_conn_id='ssh_extract')

task_extract = SSHOperator(
    task_id='extraction',
    command='python3 driver.py "initial"',
    ssh_hook=hook_extract,
    dag=dag
)

hook_clean = SSHHook(ssh_conn_id='ssh_clean')

task_populate_county = SSHOperator(
    task_id='populate_county',
    command='source ~/.bash_profile; /usr/bin/anaconda/envs/py36booyah/bin/python3 driver-population.py "initial"',
    ssh_hook=hook_clean,
    dag=dag
)

task_clean_states = SSHOperator(
    task_id='clean_states',
    command='source ~/.bash_profile; /usr/bin/anaconda/envs/py36booyah/bin/python3 driver-states.py "initial"',
    ssh_hook=hook_clean,
    dag=dag
)

task_clean_usafacts = SSHOperator(
    task_id='clean_usafacts',
    command='source ~/.bash_profile; /usr/bin/anaconda/envs/py36booyah/bin/python3 driver-usafacts.py "initial"',
    ssh_hook=hook_clean,
    dag=dag
)

task_clean_financial = SSHOperator(
    task_id='clean_financial',
    command='source ~/.bash_profile; /usr/bin/anaconda/envs/py36booyah/bin/python3 driver-financial.py "initial"',
    ssh_hook=hook_clean,
    dag=dag
)

task_extract >> task_populate_county >> task_clean_states >> task_clean_usafacts >> task_clean_financial
