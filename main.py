#!/usr/bin/python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as dates

from database import Database

from threading import Thread
import time


if __name__ == '__main__':
    ticker_file = open('./tickers')
    tickers = [ticker.strip() for ticker in ticker_file.readlines()]

    database = Database(tickers, 64)

    # update_threads = []
    tickers = ['AAPL']
    # for ticker in tickers:
    #     update_thread = Thread(target = database.update_bars, args=(ticker,), daemon=True)
    #     update_thread.start()
    #     update_threads.append(update_thread)

    def algo(ticker):
        while len(database.bars[ticker]) != database.NUM_BARS:
            database.update_bars(ticker)
            print(database.bars[ticker])

        while True:
            print(database.bars[ticker])
            print('{0}: {1}'.format(ticker, database.calculate_rsi(ticker)))

    algo_threads = []
    for ticker in tickers:
        algo_thread = Thread(target=algo, args=(ticker,))
        algo_thread.start()
        algo_threads.append(algo_thread)


        # if len(database.bars['SPY']) == database.NUM_BARS:
        #     print(database.calculate_sma('SPY', 4))
        #     print(database.calculate_ema('SPY', 4))
        #     macd = database.calculate_macd('SPY')
        #     signal = database.calculate_macd_signal_line('SPY')
        #     print(macd - signal)
        #     print('-' * 70)
