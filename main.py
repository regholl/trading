#!/usr/bin/python3
import psycopg2 as pg

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as dates

from database import Database

ticker_file = open('./tickers')
tickers = [ticker.strip() for ticker in ticker_file.readlines()]

database = Database()
for ticker in tickers:
    df = pd.read_sql('select to_timezone(timestamp, \'-05:00\') timestamp,LAST_PRICE from {0}'.format(ticker), con=database.connection)
    x = df.loc[:,"timestamp"].to_numpy()
    y = df.loc[:,"LAST_PRICE"].to_numpy()
    x = np.array(x, dtype='datetime64[ns]')

    print(ticker)
    plt.plot(x, y)
    plt.show()
