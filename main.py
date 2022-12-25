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

    tickers = ['AAPL']

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

