import ccxt

import pandas as pd
pd.set_option('display.max_rows', None)
from datetime import datetime

from ta.trend import MACD
from ta.momentum import RSIIndicator
import warnings

import schedule as schedule
import time


exchage = ccxt.binance()

def execute_connection(symbol ='ETH/USDT', timeframe = '1d'):
    raw_data=exchage.fetch_ohlcv(symbol, timeframe, limit=1000)
    #print(raw_data)

    df = pd.DataFrame(raw_data[:-1], columns = ['date', 'open', 'high', 'low', 'close', 'volume'])


    df['date'] = pd.to_datetime(df['date'], unit='ms', utc=True)
    print(f'Executing connection an data processing at ... {datetime.now().isoformat()}')
    return df

path = 'C:/Users/eaitd/Box/Elemento Alpha/Jhonatan Higuera/Foqus/Backup archivos/Precios/'


    BTC_USD=execute_connection(symbol ='BTC/BUSD', timeframe = '1h')
    ETH_USD=execute_connection(symbol ='ETH/BUSD', timeframe = '1h')
    ADA_USD=execute_connection(symbol ='ADA/BUSD', timeframe = '1h')
    XMR_USD=execute_connection(symbol ='XMR/BUSD', timeframe = '1h')
    MATIC_USD=execute_connection(symbol ='MATIC/BUSD', timeframe = '1h')
    BUSD_USD=execute_connection(symbol ='BUSD/USDT', timeframe = '1h')

    BTC_USD.to_csv( path+'BTCBUSD1h.csv', sep=';', encoding='UTF-8')
    ETH_USD.to_csv( path+'ETHBUSD1h.csv', sep=';', encoding='UTF-8')
    ADA_USD.to_csv( path+'ADABUSD1h.csv', sep=';', encoding='UTF-8')
    XMR_USD.to_csv( path+'XMRBUSD1h.csv', sep=';', encoding='UTF-8')
    MATIC_USD.to_csv( path+'MATICBUSD1h.csv', sep=';', encoding='UTF-8')
    BUSD_USD.to_csv( path+'BUSDUSDT1h.csv', sep=';', encoding='UTF-8')



df_list=pd.read_csv(path+"/Lista genesis descarga.csv", sep=';', header=0, index_col=0)
df_list

df_list1=df_list['Nemo'].values.tolist()
df_list1


for i in df_list1:

    nombre=(path+i[:]+'1h'+'.csv')

    df_hist= pd.read_csv(nombre, sep=';')

    df = execute_connection(symbol =i, timeframe = '1h')

    df_union=pd.concat([df_hist,df])drop_duplicates(subset='date', keep='last').reset_index(drop=True)

    try:
        df_union.to_csv( nombre , sep=';', encoding='UTF-8')
    except:
        df.to_csv( 'v1'+nombre, sep=';', encoding='UTF-8')


    

