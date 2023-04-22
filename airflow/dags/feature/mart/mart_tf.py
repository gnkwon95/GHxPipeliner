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
import s3fs

import nltk
from nltk.stem import WordNetLemmatizer 
from nltk.tokenize import word_tokenize


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ts_nodash", help="current date and time, 20180101T000000")
    parser.add_argument("--token", help="twitter api token")
    parser.add_argument("--aws_access_key_id", help="aws access key")
    parser.add_argument("--aws_secret_access_key", help="aws secret access key")

    return parser.parse_args()

def main():
    args = parse_args()

    token = args.token
    print("ts = ", args.ts_nodash)
    print("token = ", args.token)

    ts_nodash_str = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    startTime = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = ts_nodash_str[0:13]+ts_nodash_str[14:16]


    bucket_name="ghpipeliner"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    coins = ["aida", "bitcoin", "etherium", "ripple", "solana"]

    for coin in coins:
        response = s3_client.get_object(Bucket=bucket_name, Key=f"premart/wordcount/{filename}.csv")
        full_df = pd.read_csv(response.get("Body"), index_col = 0)
        df = full_df[full_df['key']==coin]

        tf_ = df[['word', 'count']] 

        total_wc = sum(list(df['count'])) 
        tf_['tf'] = tf_['count']/total_wc 

        tf_.to_csv('/tmp/tf.csv')

        s3_client.upload_file('/tmp/tf.csv', bucket_name, f"mart/tf/{coin}/{filename[:15]}.csv")
        s3_client.upload_file('/tmp/tf.csv', bucket_name, f"mart/tf/{coin}/latest.csv")

if __name__ == "__main__":
    main()



