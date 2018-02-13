from time import perf_counter, time, sleep
from os import path as os_path, getcwd
from sys import path as sys_path
sys_path.append(getcwd())
from data.data import engine
import pandas as pd
import requests
from json.decoder import JSONDecodeError


class BitfinexAPI:
    def __init__(self):
        self.v1_url = 'https://api.bitfinex.com/v1/{}'
        self.v2_url = 'https://api.bitfinex.com/v2/{}'

    @property
    def trade_pairs(self):
        url = self.v1_url.format('symbols')
        res = requests.get(url)
        json_res = res.json()
        return [a.upper() for a in json_res]

    def get_candles(self, symbol, time_frame, section='hist', limit=None, start=None, end=None, sort=1):
        url = self.v2_url.format('candles/trade:{}:t{}/{}'.format(time_frame, symbol, section))
        params = {}
        if limit is not None:
            params['limit'] = limit
        if start is not None:
            params['start'] = start
        if end is not None:
            params['end'] = end
        if sort is not None:
            params['sort'] = sort
        res = requests.get(url, params)
        while res.status_code != 200:
            try:
                print("API call failed, trying again in 5 seconds. {}".format(res.json()))
            except JSONDecodeError:
                print("API call failed, json decoding failed. Trying again in 5 seconds")
            sleep(5)
            res = requests.get(url, params)
        return res.json()


conn = engine.connect()

api = BitfinexAPI()

# trade_pairs = api.get_symbols()
trade_pairs = ['BTCUSD']
# cursor = conn.cursor()
time_frames = ['1m']
query_pairs = [(x, y) for x in trade_pairs for y in time_frames]
start_ts = (int(time()) - 60*60*24*31*3) * 1000 # three month

for pair, time_frame in query_pairs:
    latest_ts = start_ts
    candles = pd.DataFrame(columns=['timestamp', 'open', 'close', 'high', 'low', 'volume_btc'])
    filename = 'bitfinex'
    res = [0 for a in range(1000)]
    while len(res) == 1000:
        start = perf_counter()
        print('Getting last recieved candle for {} {}'.format(time_frame, pair))
        print('Getting {} candles for {} starting at {}'.format(time_frame, pair, latest_ts))
        res = api.get_candles(pair, time_frame, limit=1000, start=latest_ts)
        print('Fetched {} candles'.format(len(res)))
        candles = candles.append(pd.DataFrame(columns=candles.columns, data=res))
        latest_ts = candles['timestamp'].max()
        print(perf_counter() - start)
        sleep(max(0, 2-(perf_counter() - start)))

    candles['volume_currency'] = candles['volume_btc'] * candles['close']
    print(f'{filename}: saving to DB')
    candles.to_sql(filename, conn, if_exists='replace', chunksize=200)

    print(f'{filename}: modifying columns')
    conn.execute(f"""
        ALTER TABLE {filename} ALTER timestamp TYPE TIMESTAMP WITH TIME ZONE USING to_timestamp(timestamp / 1000) AT TIME ZONE 'UTC';
        CREATE INDEX {filename}_timestamp ON {filename} (timestamp);
        """)
    print(f'{filename}: done')



