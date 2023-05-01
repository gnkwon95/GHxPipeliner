from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta

base_dt = '{{ds}}'

default_args = {
    'owner': 'hblee',
    'email': ['lhb08030803@gmail.com'],
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(year=2023, month=4, day=29),
    # 'run_as_user': 'root'
}

dag = DAG(
    dag_id="train-daily-dag",
    schedule_interval="0 1 * * *", # everyday
    max_active_runs=1,
    catchup=True,
    default_args=default_args
)

start = EmptyOperator(
    task_id="start",
    dag=dag
)

check_aws = BashOperator(
    task_id="check_aws",
    dag=dag,
    bash_command="which docker"
)

bash_commands = f"""\
sudo apt-get update && \
sudo apt-get install -y --no-install-recommends apt-utils && \
sudo apt-get -y install curl && \
sudo apt-get install libgomp1 && \
python3 /opt/airflow/operators/train.py --t {base_dt}
"""
# # download docker
# sudo apt-get remove docker docker-engine docker.io containerd runc && \
# sudo apt-get install \
#     ca-certificates \
#     gnupg && \
# sudo install -m 0755 -d /etc/apt/keyrings && \
# curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
# sudo chmod a+r /etc/apt/keyrings/docker.gpg && \
# echo \
#   "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
#   "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
# sudo tee /etc/apt/sources.list.d/docker.list > /dev/null && \
# sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin && \
# sudo nohup dockerd -H unix:///var/run/docker.sock && \


pycaret_update = BashOperator(
    task_id="pycaret_update",
    dag=dag,
    bash_command=f"sudo apt-get update && sudo apt-get install -y --no-install-recommends apt-utils && sudo apt-get -y install curl && sudo apt-get install libgomp1 && python3 /opt/airflow/operators/train.py --t {base_dt}"
)

# sudo apt-get -y install libgomp1 && 

start >> check_aws >> pycaret_update