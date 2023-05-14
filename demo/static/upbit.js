const bbtn = document.getElementById('upbit');
const time_e = document.getElementById('time');
const open_e = document.getElementById('open');
const high_e = document.getElementById('high');
const low_e = document.getElementById('low');
const volume_e = document.getElementById('volume');
const close_e = document.getElementById('close');
const sentiment_e = document.getElementById('sentiment');
const new_author_rate_e = document.getElementById('new_author_rate');
const heavy_author_rate_e = document.getElementById('heavy_author_rate');

console.log("start");

function sendGet() {
    var xmlHttp = new XMLHttpRequest();
    const presentDate = new Date();
    console.log(presentDate.getTime())

    xmlHttp.open( "GET", "https://api.upbit.com/v1/candles/minutes/1?unit=1&market=KRW-BTC&count=1", false );
    xmlHttp.send( null );

    response = JSON.parse(xmlHttp.responseText)[0];
    candle_date_time_kst = response["candle_date_time_kst"];
    o = response["opening_price"];
    h = response["high_price"];
    l = response["low_price"];
    c = response["trade_price"];
    v = response["candle_acc_trade_volume"];
    console.log(`Open: ${o}\nHigh: ${h}\nLow: ${l}\nClose: ${c}\nVolume: ${v}\n`);

    time_e.value = candle_date_time_kst;
    open_e.value = o;
    high_e.value = h;
    low_e.value = l;
    close_e.value = c;
    volume_e.value = v;
}

bbtn.addEventListener("click", sendGet);