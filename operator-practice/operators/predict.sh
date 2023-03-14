#!/bin/bash
# sudo apt install -y jq

echo $(date)
UPBIT_RESULT=`curl -s -X "GET" "https://api.upbit.com/v1/candles/minutes/1?unit=1&market=KRW-BTC&count=1" | jq -r .'[0]'`

CANDLE_DATE_TIME_KST=`echo $UPBIT_RESULT | jq -r '.candle_date_time_kst'`
OPEN=`echo $UPBIT_RESULT | jq -r '.opening_price'`
HIGH=`echo $UPBIT_RESULT | jq -r '.high_price'`
LOW=`echo $UPBIT_RESULT | jq -r '.low_price'`
CLOSE=`echo $UPBIT_RESULT | jq -r '.trade_price'`
VOLUME=`echo $UPBIT_RESULT | jq -r '.candle_acc_trade_volume'`

PREDICTION=`curl -s -X "POST" "$PREDICT_URL/predict?candle_date_time_kst=$CANDLE_DATE_TIME_KST&open=$OPEN&high=$HIGH&low=$LOW&close=$CLOSE&volume=$VOLUME" | jq -r '.prediction' | jq -r .'[0]'`
echo prediction=$PREDICTION