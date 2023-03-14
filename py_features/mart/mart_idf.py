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

# 지난 1주일간 데이터만 추출 필요 (목록 가져와서 제목 시작날짜 이상인것만 계산)
def read_prefix_to_df(prefix, endtime_nodash):
    args = parse_args()

    # endtime_nodash = ts_nodash
    endtime_str = datetime.strptime(endtime_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H%M%S")
    endtime = datetime.strptime(endtime_nodash, "%Y%m%dT%H%M%S")
    starttime = endtime - timedelta(days=7)
    starttime_str = starttime.strftime("%Y-%m-%dT%H%M%S")

    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    s3 = boto3.resource('s3')
    bucket_name = 'ghpipeliner'
    bucket = s3.Bucket(bucket_name)
    
    premart_dir = prefix

    files_in_s3 = [f.key.split(premart_dir +"/")[1] for f in bucket.objects.filter(Prefix=premart_dir).all()]
    filenames = [i for i in files_in_s3 if (i <= endtime_str and i >= starttime_str)]

    full_df = []

    for f in filenames:
        response = s3_client.get_object(Bucket=bucket_name, Key=f"{premart_dir}/{f}")
        tdf = pd.read_csv(response.get("Body"), index_col = 0)
        full_df.append(tdf)
    return pd.concat(full_df)

def main():
    args = parse_args()

    token = args.token
    
    # 기존 코드
    # startTime = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # endTime = datetime.strptime(startTime, "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=7)
    # endTime_str = endTime.strftime("%Y%m%dT%H%M%S")

    # endTime으로 수정
    endTime = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=60)
    endTime_str = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # startTime = datetime.strptime(endTime_str, "%Y-%m-%dT%H:%M:%SZ") - timedelta(days=7)
    # startTime_str = endTime.strftime("%Y%m%dT%H%M%S")
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    df = read_prefix_to_df("premart/wordcount", endTime_str) # 24 * 7 개 csv 합한 큰 테이블

    coins = set(df['key'])

    for coin in coins:
        coin_df = df[df['key']==coin].rename(columns={'count':'wc'})
        coin_df['date'] = coin_df['date'].astype(str)

        unique_terms = set(df['word']) # btc -> df로 수정
        dates = sorted(list(set(df['date']))) # btc -> df로 수정

        st = []

        wc_list = {} 
        date_count = len(dates)

        for date in dates:
            tdf = coin_df[coin_df['date']==str(date)]
            wc_list[date] = dict(zip(tdf.word, tdf.wc))

        for term in unique_terms:
            row = []
            row.append(term)
            unique_count = 0
            for date in dates:
                count = wc_list[date].get(term, 0) 
                row.append(count) 
                if (count != 0):
                    unique_count +=1
                #count = 0 일 때 에러, unique_count -> 1+unique_count 변경
            row.append(np.log(date_count/1+unique_count))
            st.append(row)

        columns = []
        columns.append('word')
        for i in range(len(dates)):
            columns.append('date'+str(i))
        columns.append('idf')

        idf_ = pd.DataFrame(st, columns=columns)

        endTimeFormatted = endTime.replace(":", "")

        idf_.to_csv('/tmp/idf.csv')

        s3_client.upload_file('/tmp/idf.csv', bucket_name, f"mart/idf/{coin}/{endTimeFormatted[:15]}.csv") # 8 -> 15 변경
        s3_client.upload_file('/tmp/idf.csv', bucket_name, f"mart/idf/{coin}/latest.csv")

if __name__ == "__main__":
    main()



