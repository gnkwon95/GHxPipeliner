import boto3
import pandas as pd
import numpy as np
import json
import argparse
from datetime import datetime, timedelta
from random import random
import pytz

from pycaret.classification import *
import subprocess
import base64
from airflow.models import Variable

AWS_ACCESS_KEY = Variable.get('AWSAccessKeyId2')
AWS_SECRET_KEY = Variable.get('AWSSecretAccessKey2')

def load_data(aws_access_key, aws_secret_key, from_dt, to_dt, cols):
    dt = from_dt
    df = pd.DataFrame()
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    while(dt <= to_dt):
        dt_str = dt.strftime('%Y-%m-%d')
        print(f"Reading {dt}...")
        for hr in range(24):
            candle_file_name = f'mart/ohlcv/{dt + timedelta(hours=hr, minutes=0, seconds=0)} candles.json'
            candle_response = s3.get_object(Bucket='ghpipeliner', Key=candle_file_name)
            candle_obj = json.loads(candle_response['Body'].read().decode('utf-8'))
            
            twitter_file_name = dt_str+"T"+f"{hr:02}"+"00.csv"
            # tfidf - sentiment file
            print(twitter_file_name)
            tfidf_response = s3.get_object(Bucket='ghpipeliner', Key=f'mart/tf_idf/{twitter_file_name}')
            tfidf_obj = pd.read_csv(tfidf_response.get("Body"))

            # issue
            # issue_response = s3.get_object(Bucket='ghpipeliner', Key=f'mart/issue/{twitter_file_name}')
            # issue_obj = pd.read_csv(issue_response.get("Body"))
            
            sentiment = list(tfidf_obj[tfidf_obj['coin']=='bitcoin']['value'])[0]
            # new_author_rate = list(issue_obj[issue_obj['coin']=='bitcoin']['new_author_rate'])[0]
            # heavy_author_rate = list(issue_obj[issue_obj['coin']=='bitcoin']['heavy_author_rate'])[0]
            
            dic_with_sentiment = []

            for row in candle_obj['candles']:
                row['sentiment'] = sentiment
                row['new_author_rate'] = random()
                row['heavy_author_rate'] = random()

                dic_with_sentiment.append(row)
            df = pd.concat([df, pd.DataFrame(dic_with_sentiment)[cols]])
        dt = dt + timedelta(days=1)
            
    df.reset_index(drop=True, inplace=True)
    df['candle_date_time_kst'] = df['candle_date_time_kst'].astype(dtype=np.datetime64)
    
    return df

def preprocess_data(df, n = 15):
    df['target'] = df.shift(-n)['close']
    df.loc[df['target'] < df['close'], 'target'] = 0  # 순서 주의
    df.loc[df['target'] >= df['close'], 'target'] = 1 # 순서 주의
    data = df.iloc[:len(df) - n]
    
    return data

def train(data):
    exp_clf = setup(data=data, target='target')
    lgbm = create_model('lightgbm', silent=True)
    tuned_lgbm = tune_model(lgbm)
    final_lgbm = finalize_model(tuned_lgbm)
    print("Train end!")

    result = subprocess.run(["ls"])
    print(result.stdout)
    create_api(final_lgbm, 'lgbm_api', host='0.0.0.0')
    create_docker('lgbm_api')

def build_docker(ecr, image_tag):
    print("Update requirements.txt")
    result = subprocess.run(["pip", "freeze", "|", "grep", "-E", "(fastapi|pycaret|uvicorn)", ">", "requirements.txt"])

    print("Build Docker Image...")
    result = subprocess.run(["sudo", "docker", "image", "build", ".", "-t", image_tag])
    new_image_tag = "433166909747.dkr.ecr.ap-northeast-2.amazonaws.com/pycaret_lgbm"
    result = subprocess.run(["sudo", "docker", "image", "tag", image_tag, new_image_tag])

    print(result.stdout, result.stderr)
    print("Create Docker Manifest")
    
    image_manifest = get_image_manifest(ecr, new_image_tag)
    
    return new_image_tag, image_manifest

def get_image_manifest(ecr, image_name):
    response = ecr.get_authorization_token()
    authorization_token = response['authorizationData'][0]['authorizationToken']
    user_name, password = base64.b64decode(authorization_token).decode().split(':')

    login_cmd = f"echo {password} | sudo docker login --username AWS --password-stdin 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com"
    result = subprocess.run(login_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

    cmd = f"sudo docker manifest inspect {image_name}"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if result.returncode == 0:
        print(f"Inspect Result: {result.stdout}")
        image_manifest = result.stdout
        return image_manifest
    else:
        error_msg = f"Error getting image manifest for {image_name}: {result.stderr.strip()}"
        raise RuntimeError(error_msg)

def put_ecr(ecr, repository_name, image_manifest, image_tag):
    response = ecr.get_authorization_token()
    authorization_token = response['authorizationData'][0]['authorizationToken']
    user_name, password = base64.b64decode(authorization_token).decode().split(':')

    login_cmd = f"echo {password} | sudo docker login --username AWS --password-stdin 433166909747.dkr.ecr.ap-northeast-2.amazonaws.com"
    result = subprocess.run(login_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    # print(f"Docker Image Path: {image_path}")
    # with open(image_path, 'rb') as image_file:
    ecr.put_image(
        registryId="433166909747",
        repositoryName=repository_name,
        imageManifest=image_manifest,
        imageTag="latest"
    )

    print("put ecr")


if __name__ == "__main__":
    argp = argparse.ArgumentParser()
    argp.add_argument("--t", type=str)
    args = argp.parse_args()

    yesterday = datetime.strptime(args.t, "%Y-%m-%d") # KST
    to_dt = datetime(yesterday.year, yesterday.month, yesterday.day)
    from_dt = to_dt - timedelta(days=3)

    print("Load data...")
    cols = [
        'candle_date_time_kst', 'open', 'high', 'low', 'close', 'volume',
        'sentiment', 'new_author_rate', 'heavy_author_rate',
    ]
    df = load_data(AWS_ACCESS_KEY, AWS_SECRET_KEY, from_dt, to_dt, cols)

    print("Preprocess data...")
    data = preprocess_data(df)

    # print("Install pre-requisit library")
    # subprocess.run(["sudo", "apt-get", "-y", "install", "libgomp1"])
    
    print("Train data...")
    train(data)

    image_tag="pycaret_lgbm:latest"
    ecr = boto3.client(
        'ecr',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name='ap-northeast-2'
    )
    new_image_tag, image_manifest = build_docker(ecr, image_tag)
    print("Updating ECR Image")
    put_ecr(ecr, repository_name='pycaret_lgbm', image_manifest=image_manifest, image_tag=new_image_tag)
