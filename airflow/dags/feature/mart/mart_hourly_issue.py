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

# 지난 1주일간 데이터만 추출 필요 (목록 가져와서 제목 시작날짜 이상인것만 계산)
def read_prefix_to_df(prefix, endtime_nodash, s3_client, args):

    #endtime_nodash = 20180101T000000
    endtime_str = datetime.strptime(endtime_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H%M%S")
    endtime = datetime.strptime(endtime_nodash, "%Y%m%dT%H%M%S")
    starttime = endtime - timedelta(days=7)
    starttime_str = starttime.strftime("%Y-%m-%dT%H%M%S")


    s3 = boto3.resource("s3",
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key)
    bucket_name = 'ghpipeliner'
    bucket = s3.Bucket(bucket_name)
    
    files = bucket.objects.filter(Prefix=prefix).all()
    print("files = ")
    print(files)
    print([f.key for f in files])

    files_in_s3 = [f.key.split(prefix +"/")[1] for f in bucket.objects.filter(Prefix=prefix).all()]
    filenames = [i for i in files_in_s3 if (i <= endtime_str and i >= starttime_str)]

    print("used files = ")
    print(filenames)

    full_df = []

    for f in filenames:
        response = s3_client.get_object(Bucket=bucket_name, Key=f"{prefix}/{f}")
        tdf = pd.read_csv(response.get("Body"))
        full_df.append(tdf)
    return pd.concat(full_df)


def main():
    args = parse_args()

    token = args.token
    print("ts = ", args.ts_nodash)
    print("token = ", args.token)

    ts_nodash_str = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    ts_nodash_15_ago = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=15)).strftime("%Y%m%dT%H%M%S")
    endTime = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=15)
    endTime_str = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    def cast_to_two_digits(s):
        return s.zfill(2)

    df = read_prefix_to_df("premart/author", ts_nodash_15_ago, s3_client, args) # 24 * 7 개 csv 합한 큰 테이블
    df['date']=df['date'].astype(str)
    df['time']=df['time'].astype(str)
    df['time']=df['time'].apply(cast_to_two_digits)

    coins = set(df['key'])

    issue_rates = []

    for coin in coins:
        coin_df = df[df['key']==coin][['author_id', 'cnt', 'date', 'time']]

        print("coin df info = ", coin, len(coin_df))

        prev_df = coin_df[(coin_df['date']!=ts_nodash_15_ago[:8]) | (coin_df['time']!=ts_nodash_15_ago[9:11])]
        print(len(prev_df))
        print(prev_df[:5])

        print(prev_df[['date', 'time']].drop_duplicates(['date', 'time']))


        last_hour_df = coin_df[(coin_df['date']==ts_nodash_15_ago[:8]) & (coin_df['time']==ts_nodash_15_ago[9:11])]

        print("check time = ", ts_nodash_15_ago[:8], ts_nodash_15_ago[9:11])
        print(len(last_hour_df))
        print(ts_nodash_15_ago)
        print(last_hour_df[:5])

        prev_authors = list(set(prev_df['author_id']))
        last_hour_authors = list(set(last_hour_df['author_id']))
        new_authors = [i for i in last_hour_authors if i not in prev_authors]

        # 지난 한시간 글쓴이 중 새로 글 쓴 사람 
        new_author_rate = len(new_authors) / len(last_hour_authors)

        # 오늘 제외 글쓴 사람들
        pastdays_df = coin_df[(coin_df['date']!=ts_nodash_15_ago[:8])]
        author_activedays_df = pastdays_df.drop_duplicates(['date', 'author_id']).groupby('author_id').count().reset_index()[['author_id', 'cnt']]
        # 중 5일 이상 글 쓴 사람
        heavy_authors = list(author_activedays_df[author_activedays_df['cnt']>=5]['author_id'])

        heavy_author_last_hour = [author for author in last_hour_authors if author in heavy_authors]
        # 지난 한시간 글쓴이 중 해비유저 비율
        heavy_author_rate = len(heavy_author_last_hour)/len(last_hour_authors) 

        issue_rates.append([coin, new_author_rate, heavy_author_rate])
    
    issue_df = pd.DataFrame(issue_rates, columns=['coin', 'new_author_rate', 'heavy_author_rate'])
    issue_df['date'] = ts_nodash_15_ago[:8]
    issue_df['time'] = ts_nodash_15_ago[9:11]

    issue_df.to_csv('/tmp/issue_df.csv')


    bucket_name = "ghpipeliner"

    endTimeFormatted = ts_nodash_str.replace(":", "")
    s3_client.upload_file('/tmp/issue_df.csv', bucket_name, f"mart/issue/{endTimeFormatted[:15]}.csv")
    s3_client.upload_file('/tmp/issue_df.csv', bucket_name, f"mart/issue/latest.csv")


if __name__ == "__main__":
    main()