from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime,timedelta

base_dt = '{{ds}}'
execution_date = '{{((dag_run.logical_date + macros.timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")}}'

default_args = {
    'owner': 'hblee',
    'email': ['lhb08030803@gmail.com'],
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(year=2023, month=4, day=30),
}

dag = DAG(
    dag_id="upbit-hourly-dag",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=True,
    default_args=default_args
)

start = EmptyOperator(
    task_id="start",
    dag=dag
)

print_execution_date = BashOperator(
    task_id="print_execution_date",
    dag=dag,
    bash_command=f"echo {execution_date}"
)

upbit_update = BashOperator(
    task_id="upbit_update",
    dag=dag,
    bash_command=f"python3 /opt/airflow/operators/runner.py --m KRW-BTC --t '{execution_date}' --c 60"
)

start >> print_execution_date >> upbit_update