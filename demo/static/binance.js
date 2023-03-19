const bbtn = document.getElementById('binance');
const open_e = document.getElementById('open');
const high_e = document.getElementById('high');
const low_e = document.getElementById('low');
const volume_e = document.getElementById('volume');
const answer_e = document.getElementById('answer');

console.log("start");

function sendGet() {
    // console.log("hi!");
    var xmlHttp = new XMLHttpRequest();
    const presentDate = new Date();
    console.log(presentDate.getTime())

    xmlHttp.open( "GET", "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1", false ); // false for synchronous request
    xmlHttp.send( null );

    response = JSON.parse(xmlHttp.responseText)[0];
    o = response[1];
    h = response[2];
    l = response[3];
    c = response[4];
    v = response[5];
    console.log(`Open: ${o}\nHigh: ${h}\nLow: ${l}\nClose: ${c}\nVolume: ${v}\n`);

    open_e.value = o;
    high_e.value = h;
    low_e.value = l;
    volume_e.value = v;
    answer_e.innerText = `실제 ${c}`;
}

bbtn.addEventListener("click", sendGet);
