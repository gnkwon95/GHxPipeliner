import airflow
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import sys
import os
from test import tester

aws_access_key_id = Variable.get('AWSAccessKeyId2')
aws_secret_access_key = Variable.get('AWSSecretAccessKey2')
twitter_token = Variable.get('TwitterToken')

default_args = {
    'owner': 'knkwon',
    'email': ['gnkwon95@gmail.com'],
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
 #   'start_date': datetime(year=2022, month=6, day=24),
}

dag_hourly = DAG(
    dag_id="twitter-hourly-dag",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    default_args=default_args
)

bash_args = "--ts_nodash '{{ ts_nodash }}' \
            --token ${twitter_token} \
            --aws_access_key_id ${aws_access_key_id} \
            --aws_secret_access_key ${aws_secret_access_key}"

print_current = BashOperator(
    task_id="pwd",
    dag=dag_hourly,
    bash_command='echo "$PWD"'
)

print_ls = BashOperator(
    task_id="ls",
    dag=dag_hourly,
    bash_command='echo "$(ls)"'
)

def pathprint():
    print(os.getcwd())
    return os.getcwd()

print_path = PythonOperator(
    task_id='sys_path',
    dag=dag_hourly,
    python_callable=pathprint
)

raw_twitter = BashOperator(
    task_id="raw_twitter_hourly",
    dag=dag_hourly,
    bash_command="python3 raw_twitter_hourly.py "+ bash_args
)

# tester = BashOperator(
#     task_id="test",
#     dag=dag_hourly,
#     bash_command="python3 test.py"
# ) 

python_tester = PythonOperator(
    task_id="ptest",
    dag=dag_hourly,
    python_callable=tester
)