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

    database = Database(tickers)

    update_threads = []
    for ticker in tickers:
        update_thread = Thread(target = database.update_bars, args=(ticker,))
        update_thread.start()
        update_threads.append(update_thread)

    # bar = database.get_current_bar('SPY')
    # print(bar)
    while True:
        print(database.bars)
