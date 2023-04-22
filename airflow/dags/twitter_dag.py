import airflow
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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

dag_hourly = DAG( # dag의 execution_date default 시간을 한국으로 바꿔야함
    dag_id="twitter-hourly-dag",
    schedule_interval="0 * * * *",
    timezone='Asia/Seoul',   
    max_active_runs=1,
    default_args=default_args
)

dag_weekly = DAG(
    dag_id="twitter-weekly-dag",
    schedule_interval="0 0 * * 0",
    timezone='Asia/Seoul',   
    max_active_runs=1,
    default_args=default_args
)



bash_args = "--ts_nodash '{{ ts_nodash }}' \
            --token ${twitter_token} \
            --aws_access_key_id ${aws_access_key_id} \
            --aws_secret_access_key ${aws_secret_access_key}"

# assume access to files under path feature/
raw_twitter = BashOperator(
    task_id="raw_twitter_hourly",
    dag=dag_hourly,
    bash_command="python3 feature/raw/raw_twitter_hourly.py "+ bash_args
)

import feature.premart.premart_hourly_text


def python_job():
    print("hello world")

premart_text = PythonOperator(
    task_id="premart_hourly_text",
    dag=dag_hourly,
    python_callable=premart_hourly_text,
    op_args=bash_args
)

mart_issue = BashOperator(
    task_id="mart_issue",
    dag=dag_hourly,
    bas_command="python3 feature/mart/mart_hourly_issue.py "+ bash_args
)

mart_tf = BashOperator(
    task_id="mart_tf",
    dag=dag_hourly,
    bas_command="python3 feature/mart/mart_tf.py "+ bash_args
)

mart_sentiment = BashOperator(
    task_id="mart_sentiment",
    dag=dag_hourly,
    bas_command="python3 feature/mart/mart_sentiment.py "+ bash_args
)

mart_idf = BashOperator(
    task_id="mart_idf",
    dag=dag_weekly,
    bas_command="python3 feature/mart/mart_idf.py "+ bash_args
)

today = pendulum.today()
days_since_sunday = today.day_of_week

hourly_mart_idf_sensor = ExternalTaskSensor( 
    task_id='wait_for_idf',
    external_dag_id='twitter-weekly-dag',
    external_task_id='mart_idf',
    execution_delta=timedelta(days=days_since_sunday, hours="{{execution_date.in_timezone('Asia/Seoul').strftime('%H')}}")
    dag=dag_hourly,
)

weekly_premart_text_sensor = ExternalTaskSensor(
    task_id='wait_for_text',
    external_dag_id='twitter-hourly-dag',
    external_task_id='premart_text',
    execution_delta=timedelta(hours=0),
    dag=dag_weekly
)

# hourly jobs
raw_twitter >> premart_text >> mart_issue
premart_text >> mart_tf
hourly_mart_idf_sensor >> mart_sentiment


# weekly jobs
weekly_premart_text_sensor >> mart_idf

