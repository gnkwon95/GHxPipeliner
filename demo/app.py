import os
import requests
from flask import Flask, render_template, request
from random import randint
from datetime import datetime, timedelta

app = Flask(__name__)

@app.route("/<path:statement>", methods=['POST', 'GET'])
def predict(statement):
    options={
        "candle_date_time_kst": "2023-05-14T17:56:00",
        "open": "0.0",
        "high": "0.0",
        "low": "0.0",
        "close": "0.0",
        "volume": "0.0",
        "sentiment": "0.0",
        "new_author_rate": "0.0",
        "heavy_author_rate": "0.0",
    }
    params = statement.split('&')
    for param in params:
        if param.startswith('candle_date_time_kst='):
            options['candle_date_time_kst']=param.replace('candle_date_time_kst=', '')
        elif param.startswith('open='):
            options['open']=param.replace('open=', '')
        elif param.startswith('high='):
            options['high']=param.replace('high=', '')
        elif param.startswith('low='):
            options['low']=param.replace('low=', '')
        elif param.startswith('close='):
            options['close']=param.replace('close=', '')
        elif param.startswith('volume='):
            options['volume']=param.replace('volume=', '')
        elif param.startswith('sentiment='):
            options['sentiment']=param.replace('sentiment=', '')
        elif param.startswith('new_author_rate='):
            options['new_author_rate']=param.replace('new_author_rate=', '')
        elif param.startswith('heavy_author_rate='):
            options['heavy_author_rate']=param.replace('heavy_author_rate=', '')
    
    result = "Hi"
    if request.method == 'POST':
        print(request.form.to_dict())
        requst_form = request.form.to_dict()
        options['candle_date_time_kst'] = requst_form['time']
        options['open'] = requst_form['open']
        options['high'] = requst_form['high']
        options['low'] = requst_form['low']
        options['close'] = requst_form['close']
        options['volume'] = requst_form['volume']
        options['sentiment'] = requst_form['sentiment']
        options['new_author_rate'] = requst_form['new_author_rate']
        options['heavy_author_rate'] = requst_form['heavy_author_rate']

        print(options)
        r = requests.post(
            "http://3.35.21.35:8000/predict",
            headers={"content-type": "application/json"},
            # data=f"[{t},{o},{h},{l},{c},{v},{sentiment},{new_author_rate},{heavy_author_rate}]"
            params=options
        )
        pred = int(float(r.json()["prediction"][0]))
        print(pred)

        if pred == 0.0:
            result = 'Sell'
        elif pred == 1:
            result = 'Buy'

    t = str(options['candle_date_time_kst'])
    o = str(options['open'])
    h = str(options['high'])
    l = str(options['low'])
    c = str(options['close'])
    v = str(options['volume'])
    sentiment = str(options['sentiment'])
    new_author_rate = str(options['new_author_rate'])
    heavy_author_rate = str(options['heavy_author_rate'])

    
    
    return render_template("page.html",
        open=o,
        high=h,
        low=l,
        close=c,
        volume=v,
        sentiment=sentiment,
        new_author_rate=new_author_rate,
        heavy_author_rate=heavy_author_rate,
        result=result
    )


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
