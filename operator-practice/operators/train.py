import boto3
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import pytz

from pycaret.classification import *


def load_data(from_dt, to_dt, cols, s3):
    dt = from_dt
    df = pd.DataFrame()
    while(dt <= to_dt):
        print(f"Reading {dt}...")
        for hr in range(24):
            file_name = f'{dt + timedelta(hours=hr, minutes=5, seconds=1)} candles.json'
            try:
                response = s3.get_object(Bucket='hb-ohlcv-buy-or-sell', Key=file_name)
            except:
                file_name = f'{dt + timedelta(hours=hr, minutes=5, seconds=2)} candles.json'
                response = s3.get_object(Bucket='hb-ohlcv-buy-or-sell', Key=file_name)
            obj = json.loads(response['Body'].read().decode('utf-8'))
            df = pd.concat([df, pd.DataFrame(obj['candles'])[cols]])
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
    exp_clf = setup(data=data, target='target', silent=True)
    lgbm = create_model('lightgbm', silent=True)
    tuned_lgbm = tune_model(lgbm)
    final_lgbm = finalize_model(tuned_lgbm)
    print("Train end!")

    create_api(final_lgbm, 'pycaret/lgbm_api', host='0.0.0.0')
    create_docker('pycaret/lgbm_api')


if __name__ == "__main__":
    yesterday = datetime.now(pytz.timezone('Asia/Seoul')) - timedelta(days=1) # KST
    to_dt = datetime(yesterday.year, yesterday.month, yesterday.day)
    from_dt = to_dt - timedelta(days=3)

    session = boto3.session.Session(profile_name='default')
    s3 = session.client('s3')
    print("Load data...")
    df = load_data(from_dt, to_dt, ['candle_date_time_kst', 'open', 'high', 'low', 'close', 'volume'], s3)

    print("Preprocess data...")
    data = preprocess_data(df)

    print("Train data...")
    train(data)
