import os
import subprocess
from datetime import datetime, timezone, timedelta

F_PATH = os.path.dirname(os.path.realpath(__file__))
C_PATH = os.getcwd()

def get_candle_info(market_symbol: str, dtime: str, count: int):
    subprocess.call(['sh', F_PATH + '/ecr_run.sh', market_symbol, dtime, str(count)])

if __name__ == "__main__":
    dnow = datetime.now() + timedelta(hours=9)
    dtime = dnow.strftime("%Y-%m-%d %H:%M:%S")
    print(dtime)
    get_candle_info("KRW-BTC", dtime, 60)