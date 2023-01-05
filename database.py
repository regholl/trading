#!/usr/bin/env python3
import psycopg2 as pg
from questdb.ingress import Sender, IngressError

import pandas as pd
import pandas_ta as ta
from pandas_ta.volatility import bbands
import numpy as np

import warnings
import os
import sys

from dotenv import dotenv_values


class Database:
    # store most recent bars in a dictionary of dataframes
    bars = {}

    def __init__(self, tickers, num_bars):
        config = {
            **dotenv_values(),
            **os.environ
        }
        warnings.filterwarnings('ignore')

        self.NUM_BARS = num_bars
        self.DB_HOST = config['DB_HOST']
        self.DB_INSERT_PORT = config['DB_INSERT_PORT']
        self.DB_USER = config['DB_USER']
        self.DB_PASSWORD = config['DB_PASSWORD']
        self.DB_PORT = config['DB_PORT']

        self.connection = pg.connect(
            "user='{0}' password='{1}' host='{2}' port='{3}'".format(
                self.DB_USER,
                self.DB_PASSWORD,
                self.DB_HOST,
                self.DB_PORT
            )
        )

        self.previous_time = None
        for ticker in tickers:
            self.bars[ticker] = pd.DataFrame(columns=['open', 'close', 'low', 'high'])

    def get_current_price(self, ticker):
        return self.bars[ticker]['close'].iloc[-1]

    def get_current_bar(self, ticker):
        try:
            query = 'SELECT timestamp_floor(\'m\', to_timezone(now(), \'-05:00\')) floor, to_timezone(timestamp, \'-05:00\') timestamp_est,LAST_PRICE from {0} \
                    WHERE to_timezone(timestamp, \'-05:00\') \
                    BETWEEN timestamp_floor(\'m\', to_timezone(now(), \'-05:00\')) \
                    AND dateadd(\'m\', 1, timestamp_floor(\'m\', to_timezone(now(), \'-05:00\')))'.format(ticker)
            data = pd.read_sql(query, con=self.connection)
        except Exception:
            print('Connection lost. Reconnecting...')
            self.connection = pg.connect(
                "user='{0}' password='{1}' host='{2}' port='{3}'".format(
                    self.DB_USER,
                    self.DB_PASSWORD,
                    self.DB_HOST,
                    self.DB_PORT
                )
            )
            return self.get_current_bar(ticker)

        prices = data['LAST_PRICE']
        if len(prices) == 0:
            return None

        open_ = prices.head(1).iloc[0]
        close = prices.tail(1).iloc[0]
        low = prices.min()
        high = prices.max()

        return [open_, close, low, high]

    def get_bar(self, ticker, timestamp):
        try:
            query = 'SELECT timestamp_floor(\'m\', \'{0}\') floor, to_timezone(timestamp, \'-05:00\') timestamp_est,LAST_PRICE \
                    FROM {1} \
                    WHERE to_timezone(timestamp, \'-05:00\') \
                    BETWEEN timestamp_floor(\'m\', \'{0}\') \
                    AND dateadd(\'m\', 1, timestamp_floor(\'m\', \'{0}\'))'.format(timestamp, ticker)
            data = pd.read_sql(query, con=self.connection)
        except Exception:
            print('Connection lost. Reconnecting...')
            self.connection = pg.connect(
                "user='{0}' password='{1}' host='{2}' port='{3}'".format(
                    self.DB_USER,
                    self.DB_PASSWORD,
                    self.DB_HOST,
                    self.DB_PORT
                )
            )
            return self.get_bar(ticker, timestamp)

        prices = data['LAST_PRICE']
        if len(prices) == 0:
            return None

        open_ = prices.head(1).iloc[0]
        close = prices.tail(1).iloc[0]
        high = prices.max()
        low = prices.min()

        return [open_, close, low, high]

    # run repeatedly on each loop iteration to keep updated
    def update_bars(self, ticker):
        current_time = pd.Timestamp.now().floor('T')
        time_diff = pd.Timedelta(self.NUM_BARS - 1, 'm')
        last_time = current_time - time_diff if self.previous_time is None else self.previous_time

        if len(self.bars[ticker]) > self.NUM_BARS:
            self.bars[ticker] = self.bars[ticker].iloc[1:, :]
        while last_time < pd.Timestamp.now().floor('T'):
            bar = self.get_bar(ticker, last_time)

            # if the queue is full, drop the first row through FIFO principal
            if len(self.bars[ticker]) >= self.NUM_BARS:
                self.bars[ticker] = self.bars[ticker].iloc[1:, :]

            self.bars[ticker].loc[last_time] = bar
            last_time += pd.Timedelta(1, 'm')

        self.bars[ticker].loc[last_time] = self.get_bar(ticker, last_time)
        self.bars[ticker] = self.bars[ticker].sort_index(axis=0)
        self.previous_time = last_time

    def calculate_sma(self, ticker, count):
        closing_prices = self.bars[ticker]['close']
        if len(closing_prices) < count:
            return 0
        else:
            return np.average(closing_prices[-count:])

    def calculate_ema(self, *args):
        if len(args) == 2:
            current_time = self.bars[args[0]].iloc[-1:].index
            return self.calculate_ema_recursive(args[0], args[1], current_time)
        elif len(args) == 3:
            return self.calculate_ema_recursive(args[0], args[1], args[2])

    def calculate_ema_recursive(self, ticker, count, timestamp):
        if count == 0:
            return 0
        current_price = float(self.bars[ticker].loc[timestamp, 'close'])
        k = 2.0 / (count + 1)
        return (current_price * k) + \
            ((1 - k) * self.calculate_ema_recursive(ticker, count - 1, timestamp - pd.Timedelta(1, 'm')))

    # def calculate_macd(self, ticker):
    #     current_time = self.bars[ticker].iloc[-1:].index
    #     return self.calculate_macd_at_timestamp(ticker, current_time)

    def macd(self, ticker):
        return ta.momentum.macd(self.bars[ticker]['close'])['MACDh_12_26_9'].iloc[-1]

    def calculate_macd_signal_line(self, ticker):
        current_time = self.bars[ticker].iloc[-1:].index
        return self.calculate_macd_signal_line_recursive(ticker, 9, current_time)

    def calculate_macd_signal_line_recursive(self, ticker, count, timestamp):
        if count == 0:
            return 0
        current_macd = self.calculate_macd_at_timestamp(ticker, timestamp)
        k = 2.0 / (count + 1)
        return (current_macd * k) + \
            ((1 - k) * self.calculate_macd_signal_line_recursive(ticker, count - 1, timestamp - pd.Timedelta(1, 'm')))

    def calculate_macd_at_timestamp(self, ticker, timestamp):
        return self.calculate_ema(ticker, 12, timestamp) - self.calculate_ema(ticker, 26, timestamp)

    # def calculate_rsi(self, ticker, periods=14):
    #     diffs = self.bars[ticker].diff(1)['close'].iloc[self.NUM_BARS - periods:]
    #     gains = diffs.clip(lower=0)
    #     losses = diffs.clip(upper=0).abs()
    #     avg_gain = gains.rolling(window=periods, min_periods=periods).mean()[:periods+1]
    #     avg_loss = losses.rolling(window=periods, min_periods=periods).mean()[:periods+1]

    #     for i, row in enumerate(avg_gain.iloc[periods+1:]):
    #         avg_gain.iloc[i + periods + 1] =\
    #             (avg_gain.iloc[i + periods] *
    #             (periods - 1) +
    #             gains.iloc[i + periods + 1])\
    #             / periods

    #     for i, row in enumerate(avg_loss.iloc[periods+1:]):
    #         avg_loss.iloc[i + periods + 1] =\
    #             (avg_loss.iloc[i + periods] *
    #             (periods - 1) +
    #             losses.iloc[i + periods + 1])\
    #             / periods

    #     rs = avg_gain / avg_loss
    #     rsi = 100 - (100 / (1.0 + rs))

    #     try:
    #         return rsi.iloc[periods - 1]
    #     except Exception:
    #         return None

    def rsi(self, ticker, periods=14):
        return ta.momentum.rsi(close=self.bars[ticker]['close'], length=periods).iloc[-1]

    def bollinger_bands(self, ticker):
        df = bbands(close=self.bars[ticker]['close'], length=20, offset=1)
        df['lower'] = -1
        df['upper'] = sys.maxsize
        return df

    def reset_trades(self):
        query = 'DROP TABLE IF EXISTS Trades;'
        with self.connection.cursor() as cur:
            cur.execute(query)

        query = 'CREATE TABLE Trades(ticker string, profit float, buy_price float, sell_price float, min_rsi float, buy_time timestamp, sell_time timestamp);'
        with self.connection.cursor() as cur:
            cur.execute(query)

    def get_trades(self, ticker):
        query = 'SELECT * \
                FROM Trades \
                WHERE ticker=\'{0}\''.format(ticker)
        data = pd.read_sql(query, con=self.connection)
        return data

    def send_trade(self, ticker, profit, buy_price, sell_price, min_rsi, buy_time, sell_time):
        try:
            with Sender(self.DB_HOST, self.DB_INSERT_PORT) as sender:
                sender.row(
                    'Trades',
                    columns={
                        'ticker': ticker,
                        'profit': profit,
                        'buy_price': buy_price,
                        'sell_price': sell_price,
                        'min_rsi': min_rsi,
                        'buy_time': buy_time,
                        'sell_time': sell_time
                    }
                )
                sender.flush()
        except IngressError as e:
            sys.stderr.write(f'Got error: {e}\n')

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
