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
import time

import nltk
from nltk.stem import WordNetLemmatizer 
from nltk.tokenize import word_tokenize
from collections import Counter


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ts_nodash", help="current date and time, 20180101T000000")
    parser.add_argument("--token", help="twitter api token")
    parser.add_argument("--aws_access_key_id", help="aws access key")
    parser.add_argument("--aws_secret_access_key", help="aws secret access key")

    return parser.parse_args()


def clean_tweet(tweet): 
    ''' 
    Utility function to clean tweet text by removing links, special characters 
    using simple regex statements. 
    '''
    tweet = str(tweet).lower()
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

def main():
    args = parse_args()

    token = args.token
    print("ts = ", args.ts_nodash)
    print("token = ", args.token)
    time.sleep(60*5)

    ts_nodash_str = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    startTime = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
    filename = ts_nodash_str[0:13]+ts_nodash_str[14:16]
    print("filename = ", filename)

    bucket_name="ghpipeliner"
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )


    response = s3_client.get_object(Bucket=bucket_name, Key=f"hourly_twits/{filename}.csv")

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        query_df = pd.read_csv(response.get("Body"))
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")

    query_df['clean_text'] = query_df['text'].apply(clean_tweet)
    query_df['tokenized'] = query_df['clean_text'].apply(word_tokenize)
    query_df = query_df[~query_df['key'].isnull()]
    query_df['author_id'] = query_df['author_id'].astype(int)
    query_df['id'] = query_df['id'].astype(int)

    cnt_df_storage = []
    author_df_storage = []

    keys = set(query_df['key'])

    for key in keys:
        tdf = query_df[query_df['key']==key]
        
        words = list(tdf['tokenized'])
        flat_words = [num for sublist in words for num in sublist]
        cnter = Counter(flat_words)
        cnt_df = pd.DataFrame.from_dict(cnter, orient='index').reset_index()
        cnt_df.columns=['word', 'count']
        cnt_df['key']=key
        cnt_df['date']=args.ts_nodash[:8]
        cnt_df['time']=args.ts_nodash[9:11]
        cnt_df_storage.append(cnt_df)
        
        author_cnter = Counter(tdf['author_id'])
        author_cnt_df = pd.DataFrame.from_dict(author_cnter, orient='index').reset_index()
        author_cnt_df.columns=['author_id', 'cnt']
        author_cnt_df['key']=key
        author_cnt_df['date']=args.ts_nodash[:8]
        author_cnt_df['time']=args.ts_nodash[9:11]
        author_df_storage.append(author_cnt_df)
        
    wc_df = pd.concat(cnt_df_storage)
    author_df = pd.concat(author_df_storage)


    wc_df.to_csv('wc.csv')
    author_df.to_csv('author.csv')


    s3_client.upload_file('wc.csv', bucket_name, f"premart/wordcount/{filename}.csv")
    s3_client.upload_file('author.csv', bucket_name, f"premart/author/{filename}.csv")

    print("file upload complete")

if __name__ == "__main__":
    main()



