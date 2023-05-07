import airflow
import pendulum
from pytz import timezone
from datetime import datetime,timedelta
from airflow import DAG, macros
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
import sys
import os
from airflow.models import Variable

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
    max_active_runs=1,
    default_args=default_args,
    render_template_as_native_obj=True,
)

dag_weekly = DAG(
    dag_id="twitter-weekly-dag",
    schedule_interval="0 0 * * 0", 
    max_active_runs=1,
    default_args=default_args,
    start_date=datetime.now() - timedelta(days=10),
    render_template_as_native_obj=True,
    catchup=True
)


def get_bash_args(twitter_token, aws_access_key_id, aws_secret_access_key):
    return f"""--ts_nodash "{{{{execution_date.strftime("%Y%m%dT%H%M%S")}}}}"  --token { twitter_token } --aws_access_key_id { aws_access_key_id } --aws_secret_access_key { aws_secret_access_key }"""

bash_args = get_bash_args(twitter_token, aws_access_key_id, aws_secret_access_key)

# assume access to files under path feature/
raw_twitter = BashOperator(
    task_id="raw_twitter_hourly",
    dag=dag_hourly,
    bash_command="python3 /opt/airflow/dags/feature/raw/raw_twitter_hourly.py "+ bash_args
)

premart_text = BashOperator(
    task_id="premart_hourly_text",
    dag=dag_hourly,
    bash_command="python3 /opt/airflow/dags/feature/premart/premart_hourly_text.py "+ bash_args
)

mart_issue = BashOperator(
    task_id="mart_issue",
    dag=dag_hourly,
    bash_command="python3 /opt/airflow/dags/feature/mart/mart_hourly_issue.py "+ bash_args
)

mart_tf = BashOperator(
    task_id="mart_tf",
    dag=dag_hourly,
    bash_command="python3 /opt/airflow/dags/feature/mart/mart_tf.py "+ bash_args
)

mart_sentiment = BashOperator(
    task_id="mart_sentiment",
    dag=dag_hourly,
    bash_command="python3 /opt/airflow/dags/feature/mart/mart_sentiment.py "+ bash_args
)

mart_idf = BashOperator(
    task_id="mart_idf",
    dag=dag_weekly,
    bash_command="python3 /opt/airflow/dags/feature/mart/mart_idf.py "+ bash_args
)

today = pendulum.today()
days_since_sunday = int(today.day_of_week)
hours_past = datetime.now(timezone('Asia/Seoul')).strftime('%H')

def write_hours_since_sunday(**kwargs):
    execution_date = kwargs['execution_date']
    dt = datetime.strptime(execution_date, '%Y%m%dT%H%M%S')
    now = pendulum.instance(dt)

    last_sunday = now.previous(pendulum.SUNDAY)
    hours_since_sunday = now.diff(last_sunday).in_hours()%(24*7)

    print(now)
    print(last_sunday)
    print(hours_since_sunday)

    Variable.set("hours_since_sunday", hours_since_sunday)


SensorTimeSet = PythonOperator(
    task_id='write_hours_since_sunday',
    python_callable=write_hours_since_sunday, 
    op_kwargs={'execution_date': "{{((execution_date + macros.timedelta(hours=9))).strftime('%Y%m%dT%H%M%S')}}"},
    dag=dag_hourly
)

hourly_mart_idf_sensor = ExternalTaskSensor( 
    task_id='wait_for_idf',
    external_dag_id='twitter-weekly-dag',
    external_task_id='mart_idf',
    execution_delta=timedelta(hours=int(Variable.get("hours_since_sunday"))),
    dag=dag_hourly,
)

weekly_premart_text_sensor = ExternalTaskSensor(
    task_id='wait_for_text',
    external_dag_id='twitter-hourly-dag',
    external_task_id='premart_hourly_text',
    execution_delta=timedelta(hours=0),
    dag=dag_weekly
)

# hourly jobs
raw_twitter >> premart_text >> mart_issue
premart_text >> mart_tf
SensorTimeSet >> hourly_mart_idf_sensor >> mart_sentiment


# weekly jobs
weekly_premart_text_sensor >> mart_idf

