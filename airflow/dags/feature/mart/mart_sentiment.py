import requests
import sys
import pandas as pd
import pytz
from datetime import datetime
from datetime import timedelta
import requests
import boto3
from io import StringIO
import argparse
import re
import numpy as np

import nltk
from nltk.stem import WordNetLemmatizer 
from nltk.tokenize import word_tokenize

from textblob import TextBlob


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ts_nodash", help="current date and time, 20180101T000000")
    parser.add_argument("--token", help="twitter api token")
    parser.add_argument("--aws_access_key_id", help="aws access key")
    parser.add_argument("--aws_secret_access_key", help="aws secret access key")

    return parser.parse_args()

def getPolarity(text):
    return TextBlob(text).sentiment.polarity

def main():
    args = parse_args()

    token = args.token
    print("ts = ", args.ts_nodash)
    print("token = ", args.token)

    ts_nodash_str = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    startTime = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = ts_nodash_str[0:13]+ts_nodash_str[14:16]
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    s3 = boto3.resource('s3')
    bucket_name = 'ghpipeliner'
    bucket = s3.Bucket(bucket_name)
    
    coins = ["aida", "bitcoin", "etherium", "ripple", "solana"]

    full_df = []

    for coin in coins:
        # tf 파일
        response = s3_client.get_object(Bucket=bucket_name, Key=f"mart/idf/{coin}/latest.csv")
        idf_df = pd.read_csv(response.get("Body"), index_col = 0)
        
        # idf 파일
        response = s3_client.get_object(Bucket=bucket_name, Key=f"mart/tf/{coin}/latest.csv")
        tf_df = pd.read_csv(response.get("Body"), index_col = 0)

        tf_idf_df = pd.merge(tf_df, idf_df, how = 'left')
        tf_idf_df['idf'] = tf_idf_df['idf'].fillna(tf_idf_df['idf'].max())
        tf_idf_df['tf_idf'] = tf_idf_df['tf']*tf_idf_df['idf']

        # sentiment
        tf_idf_df['word'] = tf_idf_df['word'].astype(str)
        tf_idf_df['sentiment'] = tf_idf_df['word'].apply(getPolarity)

        # 최종 value
        tf_idf_df['weighted_sentiment'] = tf_idf_df['sentiment']*tf_idf_df['tf_idf']
        
        df = pd.DataFrame({'coin': [coin], 'value': [tf_idf_df['weighted_sentiment'].sum()]})
        full_df.append(df)

    tf_idf_ = pd.concat(full_df)

    tf_idf_.to_csv('/tmp/tf_idf.csv')

    s3_client.upload_file('/tmp/tf_idf.csv', bucket_name, f"mart/tf_idf/{filename[:15]}.csv")
    s3_client.upload_file('/tmp/tf_idf.csv', bucket_name, f"mart/tf_idf/latest.csv")
    
if __name__ == "__main__":
    main()