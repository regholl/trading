#!/usr/bin/env python3
import psycopg2 as pg

import pandas as pd
import numpy as np

import warnings
import os

from dotenv import dotenv_values


class Database:

    BAR_SIZE = 128
    # store most recent bars in a queue structure
    bars = {}

    def __init__(self):
        config = {
            **dotenv_values(),
            **os.environ
        }
        warnings.filterwarnings('ignore')

        self.connection = pg.connect(
            "user='{0}' password='{1}' host='{2}' port='{3}'".format(
                config['DB_USER'],
                config['DB_PASSWORD'],
                config['DB_HOST'],
                config['DB_PORT']
            )
        )

    def get_current_bar(self, ticker):
        query = 'SELECT timestamp_floor(\'m\', to_timezone(now(), \'-05:00\')) floor, dateadd(\'m\', 1, timestamp_floor(\'m\', to_timezone(now(), \'-05:00\'))) ceil, to_timezone(timestamp, \'-05:00\') timestamp_est,LAST_PRICE from {0} \
                WHERE to_timezone(timestamp, \'-05:00\') \
                BETWEEN timestamp_floor(\'m\', to_timezone(now(), \'-05:00\')) \
                AND dateadd(\'m\', 1, timestamp_floor(\'m\', to_timezone(now(), \'-05:00\')))'.format(ticker)
        data = pd.read_sql(query, con=self.connection)

        prices = data['LAST_PRICE']
        if len(prices) == 0:
            return None

        open_ = prices.head(1).iloc[0]
        close = prices.tail(1).iloc[0]
        high = prices.max()
        low = prices.min()

        time_start = data['floor'].iloc[0]
        time_end = data['ceil'].iloc[0]

        return Bar(open_, close, high, low, time_start, time_end)

    # run as thread in background to have updated bars in memory
    def update_bars(self, ticker):
        last_minute = None
        while True:
            bar = self.get_current_bar(ticker)
            if bar == None:
                continue

            if ticker not in self.bars or last_minute == None:
                self.bars[ticker] = [bar]
                last_minute = bar._time_start

            # elif self.bars[ticker][-1]._time_start < pd.Timestamp.now().floor('T'):
            elif last_minute < pd.Timestamp.now().floor('T'):
                if len(self.bars[ticker]) >= self.BAR_SIZE:
                    self.bars[ticker].pop(0)
                self.bars[ticker].append(bar)
                last_minute = bar._time_start

            else:
                self.bars[ticker][-1] = bar
            print(self.bars[ticker])

    def calculate_sma(self, ticker, count):
        return np.average([float(bar) for bar in self.bars[ticker]])

    def __str__(self):
        return 'Connection Status: {0}\nBars: {1}'.format(self.connection.status, self.bars)


class Bar:

    def __init__(self, open_: float, close: float, high: float, low: float, time_start, time_end):
        self._open = open_
        self._close = close
        self._high = high
        self._low = low
        self._time_start = time_start
        self._time_end = time_end

    def __str__(self):
        return 'Range: {0} --- {1}\nOpen: {2}\nClose: {3}\nHigh: {4}\nLow {5}'.format(
            self._time_start,
            self._time_end,
            self._open,
            self._close,
            self._high,
            self._low
        )

    def __float__(self):
        return self._close
