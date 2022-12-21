#!/usr/bin/python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as dates

from database import Database

from threading import Thread

if __name__ == '__main__':
    ticker_file = open('./tickers')
    tickers = [ticker.strip() for ticker in ticker_file.readlines()]

    database = Database()

    update_threads = []
    tickers = ['SPY']
    for ticker in tickers:
        update_thread = Thread(target = database.update_bars, args=(ticker,))
        update_thread.start()
        update_threads.append(update_thread)

    # while True:
    #     if 'SPY' in database.bars:
    #         print(len(database.bars['SPY']))
    #         print()
            # print(database.calculate_sma('SPY', 1))
        # if 'SPY' in database.bars:
            # print(database.bars['SPY'])
    # for ticker in tickers:
    #     df = pd.read_sql('select to_timezone(timestamp, \'-05:00\') timestamp,LAST_PRICE from {0}'.format(ticker), con=database.connection)
    #     x = df['timestamp'].to_numpy()
    #     y = df['LAST_PRICE'].to_numpy()
    #     x = np.array(x, dtype='datetime64[ns]')

        # print(ticker)
        # plt.plot(x, y)
        # plt.show()
