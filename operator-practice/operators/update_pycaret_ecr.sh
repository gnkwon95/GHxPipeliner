#!/bin/bash
# load data -> train pycaret model
date
echo "Started"
source ~/anaconda3/etc/profile.d/conda.sh
conda activate py38
cd /home/ubuntu/airflow/operators/
python train.py

echo "update requiremets.txt to local pip freeze"
pip freeze | grep -E "(fastapi|pycaret|uvicorn)" > requirements.txt

echo "Build Docker Image..."
sudo docker image build . -t pycaret_lgbm:latest

# push docker image
sudo docker tag pycaret_lgbm:latest 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com/pycaret_lgbm
aws ecr get-login-password --region ap-northeast-2 | sudo docker login --username AWS --password-stdin 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com
sudo docker push 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com/pycaret_lgbm

echo "Docker Image Pushed"
# move to home dir
cd ~

# delete all docker images for device space
sudo docker rmi $(sudo docker images -q) -f
