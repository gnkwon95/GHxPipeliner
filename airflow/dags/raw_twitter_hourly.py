import requests
import sys
import pandas as pd
import pytz
from datetime import datetime
from datetime import timedelta
import requests
import boto3
from io import StringIO

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

    endTime = datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S").strftime("%Y-%m-%dT%H:%M:%SZ")
    startTime = (datetime.strptime(args.ts_nodash, "%Y%m%dT%H%M%S") - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {"query":"#btc", # "#tesla OR #ElonMusk"
            "tweet.fields":"created_at",
            "expansions":"author_id",
            "user.fields":"created_at",
            "start_time":startTime,
            "end_time":endTime,
            "max_results":"100"
            }

    query_dics = {"bitcoin": "#btc",
                "etherium": "#eth",
                "ripple": "#xrp",
                "aida": "#ada",
                "solana": "#sol"}   

    count_storage = []
    query_storage = []

    for key, query in query_dics.items():
        total_tweets = 0
        has_next = True
        count_tmp = []
        query_tmp = []
        next_token = None

        while(has_next):
            params = {"query":query, # "#tesla OR #ElonMusk"
                "tweet.fields":"created_at",
                "expansions":"author_id",
                "user.fields":"created_at",
                "start_time":startTime,
                "end_time":endTime,
                "max_results":"100"
                }

            if (next_token is not None):
                params['next_token']=next_token

            r = requests.get("https://api.twitter.com/2/tweets/search/recent", 
                        params=params,
                        headers={'Authorization': 'Bearer {}'.format(args.token)})


            response_json = r.json()
            total_tweets += response_json['meta']['result_count']
            if ("data" not in r.json()):
                break
            query_tmp.extend(r.json()['data']) # 함수로 변경해서 dataframe에 추가, csv파일로 저장. 5분마다 실행

            if 'next_token' in response_json['meta']:
                next_token = response_json['meta']['next_token']
            else:
                print("end of token")
                has_next = False

        count_storage.append([key, startTime, total_tweets])
        for i in query_tmp:
            i['key']=key
        query_storage.extend(query_tmp)

    count_df = pd.DataFrame(count_storage)
    query_df = pd.DataFrame(query_storage)
    query_df = query_df[['key', 'id', 'author_id', 'created_at', 'text']]

    count_df.to_csv('/tmp/counts.csv')
    query_df.to_csv('/tmp/twits.csv')

    bucket_name="pipeliner-kwon"

    s3 = boto3.client(
        's3',
        aws_access_key_id=args.aws_access_key_id,
        aws_secret_access_key=args.aws_secret_access_key
    )

    startTimeFormatted = startTime.replace(":", "")

    s3.upload_file('/tmp/twits.csv', bucket_name, f"hourly_twits/{startTimeFormatted[:15]}.csv")
    s3.upload_file('/tmp/counts.csv', bucket_name, f"hourly_twit_counts/{startTImeFormatted[:15]}.csv")

    print("Put object to S3 complete")

if __name__ == "__main__":
    main()