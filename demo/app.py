import sys
import os
import requests

from flask import Flask, render_template, request
app = Flask(__name__)

@app.route("/<path:statement>", methods=['POST', 'GET'])
def predict(statement):
    options={
        "open": "21195.60",
        "high": "21205.46",
        "low": "21187.19",
        "volume": "78.80792"
    }
    params = statement.split('&')
    for param in params:
        if param.startswith('open='):
            options['open']=param.replace('open=', '')
        elif param.startswith('high='):
            options['high']=param.replace('high=', '')
        elif param.startswith('low='):
            options['low']=param.replace('low=', '')
        elif param.startswith('volume='):
            options['volume']=param.replace('volume=', '')

    if request.method == 'POST':
        options['open'] = request.form['open']
        options['high'] = request.form['high']
        options['low'] = request.form['low']
        options['volume'] = request.form['volume']

    o = str(options['open'])
    h = str(options['high'])
    l = str(options['low'])
    v = str(options['volume'])

    r = requests.post(
        "http://3.35.228.37:3000/predict",
        headers={"content-type": "application/json"},
        data=f"[[{o},{h},{l},{v}]]")
    pred = r.json()[0]

    return render_template("page.html", 
                            open=o,
                            high=h,
                            low=l,
                            volume=v,
                            pred=pred)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5123)