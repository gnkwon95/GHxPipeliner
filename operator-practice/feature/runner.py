import requests
from datetime import datetime, timedelta, timezone
import json
import argparse
import boto3
import os
from candle import Candle
from time import sleep

SERVER_URL = 'https://api.upbit.com'

KST = timezone(timedelta(hours=9))

def get_decision_type(candle: dict, history_cnt=5) -> list:
    c = Candle(o = candle['open'], h = candle['high'], l = candle['low'], c = candle['close'])
    market = candle['market']
    dtime = candle['candle_date_time_kst']
    history_list = get_ohlcv(market, dtime, history_cnt)
    fop, fcp, lop, lcp = history_list[0]['open'], history_list[0]['close'], history_list[-1]['open'], history_list[-1]['close']
    c.determine_history(fop, fcp, lop, lcp)

    return c.get_buy_or_sell()

def get_ohlcv(market:str, dtime: str, count=1, unit=1) -> list:
    url = f"{SERVER_URL}/v1/candles/minutes/1?unit={unit}&market={market}&to={dtime}&count={count}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    resp_list = json.loads(response.text)
    ohlcv_list = []
    for tick in resp_list:
        ohlcv_list.append(
            {
                'market': tick['market'],
                'candle_date_time_kst': tick['candle_date_time_kst'],
                'timestamp': tick['timestamp'],
                'open': tick['opening_price'],
                'high': tick['high_price'],
                'low': tick['low_price'],
                'close': tick['trade_price'],
                'volume': tick['candle_acc_trade_volume'],
            }
        )
    ohlcv_list.reverse()
    return ohlcv_list

def put_s3(aws_access_key, aws_secret_key, bucket_name, file_name, data):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    s3.put_object(
        Bucket=bucket_name,
        Key=f'{file_name}.json',
        Body=bytes(json.dumps(data).encode('UTF-8'))
    )
    print("put s3")


def main():
    argp = argparse.ArgumentParser()
    argp.add_argument("--m", help="which market and which crypto", type=str)
    argp.add_argument("--t", help="datetime format: '%Y-%m-%d %H:%M:%S'", type=str)
    argp.add_argument("--c", help="how many ticks", type=int)

    args = argp.parse_args()
    candles = get_ohlcv(args.m, args.t, args.c)
    decision_list = []
    for candle in candles:
        decision_list.append(get_decision_type(candle, 10))
        sleep(1) # for api call restriction

    print(f"Candles Count: {len(decision_list)}")
    print(f"Latest Decision: {decision_list[-1]}")

    aws_access_key = os.environ['AWS_ACCESS_KEY']
    aws_secret_key = os.environ['AWS_SECRET_KEY']
    put_s3(aws_access_key, aws_secret_key, 'hb-ohlcv-buy-or-sell', f'{args.t} candles', {'candles': candles})
    put_s3(aws_access_key, aws_secret_key, 'hb-ohlcv-buy-or-sell', f'{args.t} decision v1', {'decision': decision_list})

if __name__ == "__main__":
    main()