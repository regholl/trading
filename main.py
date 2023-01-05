#!/usr/bin/python3
from database import Database

from threading import Thread
import math
import pandas as pd


ticker_file = open('./tickers')
tickers = [ticker.strip() for ticker in ticker_file.readlines()]

# tickers = ['AAPL']

database = Database(tickers, 64)
database.reset_trades()

bounds = {
    'lower': 'BBL_20_2.0',
    'mid': 'BBM_20_2.0',
    'upper': 'BBU_20_2.0'
}


# change algo to hold for longer periods of time instead of using scalping strategy
def uptrend_old(ticker, lower_bound=None, upper_bound=None):
    print('Starting uptrend on {0} from {1} to {2}'.format(ticker, lower_bound, upper_bound))
    holding = False
    buy_price = 0
    num_shares = 0

    lower_bound = bounds[lower_bound] if lower_bound is not None else 'lower'
    upper_bound = bounds[upper_bound] if upper_bound is not None else 'upper'

    database.update_bars(ticker)
    b_bands = database.bollinger_bands(ticker)

    # add logic for price levels given purchase history
    while (database.get_current_price(ticker) > b_bands[lower_bound].iloc[-1]) and (database.get_current_price(ticker) < b_bands[upper_bound].iloc[-1]):
        holding = True
        buy_price = database.get_current_price(ticker)
        num_shares = math.ceil(1000 / buy_price)
        buy_time = pd.Timestamp.now()
        print('Bought {0} shares of {1} at {2}'.format(num_shares, ticker, buy_price))

        while (database.get_current_price(ticker) > b_bands[lower_bound].iloc[-1]) and (database.get_current_price(ticker) < b_bands[upper_bound].iloc[-1]):
            current_price = database.get_current_price(ticker)
            if math.isnan(current_price):
                continue
            if current_price > (1.001 * buy_price) or current_price < (0.999 * buy_price):
                sell_time = pd.Timestamp.now()
                print('Sold {0} shares of {1} at {2} for a {3} of {4}'.format(
                    num_shares,
                    ticker,
                    current_price,
                    'profit' if current_price > buy_price else 'loss',
                    abs(current_price - buy_price) * num_shares))
                database.send_trade(ticker, (current_price - buy_price) * num_shares, buy_price, current_price, buy_time, sell_time)

                holding = False
                break

            database.update_bars(ticker)
            b_bands = database.bollinger_bands(ticker)
        database.update_bars(ticker)
        b_bands = database.bollinger_bands(ticker)

    exit_type = -1 if database.get_current_price(ticker) < b_bands[lower_bound].iloc[-1] else 1

    if holding:
        sell_price = database.get_current_price(ticker)
        sell_time = pd.Timestamp.now()
        while math.isnan(sell_price) or sell_price is None:
            sell_price = database.get_current_price(ticker)
        print('Sold {0} shares of {1} at {2} for a total {3} of {4}'.format(
            num_shares,
            ticker,
            sell_price,
            'profit' if sell_price > buy_price else 'loss',
            abs(sell_price - buy_price) * num_shares))

        database.send_trade(ticker, (sell_price - buy_price) * num_shares, buy_price, sell_price, buy_time, sell_time)
        holding = False

    return exit_type


def uptrend(ticker, min_rsi):
    print('Starting uptrend on {0}'.format(ticker))
    buy_price = 0
    num_shares = 0

    database.update_bars(ticker)

    buy_price = database.get_current_price(ticker)
    num_shares = math.ceil(1000.0 / buy_price)
    buy_time = pd.Timestamp.now()
    print('Bought {0} shares of {1} at {2}'.format(num_shares, ticker, buy_price))

    # buy, then break on two consecutive red bars including current bar
    # or when max price since uptrend drops below certain threshold
    # threshold is relative to fixed percentage of daily change
    open_price = database.get_bar(ticker, buy_time.floor('D') + pd.Timedelta(9, 'h') + pd.Timedelta(30, 'm'))[0]
    max_risk = abs(open_price - buy_price) * 0.15

    prev_bar_1m_open = database.bars[ticker]['open'].iloc[-1]
    prev_bar_1m_close = database.bars[ticker]['close'].iloc[-1]
    prev_bar_2m_open = database.bars[ticker]['open'].iloc[-2]
    prev_bar_2m_close = database.bars[ticker]['close'].iloc[-2]

    current_price = database.get_current_price(ticker)
    b_bands = database.bollinger_bands(ticker)
    max_close = current_price

    # TODO: adjust entry and closing parameters
    # 1. adjust entry to trigger when within range of closing above lower band, not exactly above
    # 2. change metric for closing to follow trend fully, increase loss % required to close
    while ((prev_bar_1m_close > prev_bar_1m_open) or (prev_bar_2m_close > prev_bar_2m_open)) and (max_close - max_risk < current_price) and (current_price < b_bands[bounds['upper']].iloc[-1]):
        database.update_bars(ticker)
        prev_bar_1m_open = database.bars[ticker]['open'].iloc[-1]
        prev_bar_1m_close = database.bars[ticker]['close'].iloc[-1]
        prev_bar_2m_open = database.bars[ticker]['open'].iloc[-2]
        prev_bar_2m_close = database.bars[ticker]['close'].iloc[-2]
        current_price = database.get_current_price(ticker)
        max_close = max(max_close, prev_bar_1m_close)
        b_bands = database.bollinger_bands(ticker)

    sell_time = pd.Timestamp.now()
    print('Sold {0} shares of {1} at {2} for a {3} of {4}'.format(
        num_shares,
        ticker,
        current_price,
        'profit' if current_price > buy_price else 'loss',
        abs(current_price - buy_price) * num_shares))
    database.send_trade(ticker, (current_price - buy_price) * num_shares, buy_price, current_price, min_rsi, buy_time, sell_time)


def downtrend(ticker):
    print('Starting downtrend on {0}'.format(ticker))
    while database.get_current_price(ticker) < database.bollinger_bands(ticker)[bounds['lower']].iloc[-1]:
        database.update_bars(ticker)
    print('Potential uptrend starting on {0}'.format(ticker))

    # wait until previous bar is positive and has closed above lower band
    previous_open = database.bars[ticker]['open'].iloc[-2]
    previous_close = database.bars[ticker]['close'].iloc[-2]
    min_rsi = database.rsi(ticker)
    start_time = database.bars[ticker].index[-1]
    while (previous_open > previous_close) or (previous_close < database.bollinger_bands(ticker)[bounds['lower']].iloc[-1]) or (database.bars[ticker].index[-1] == start_time):
        database.update_bars(ticker)
        previous_open = database.bars[ticker]['open'].iloc[-2]
        previous_close = database.bars[ticker]['close'].iloc[-2]
        min_rsi = min(min_rsi, database.rsi(ticker))

    print('Crossed lower band for {0} with min rsi of {1}'.format(ticker, min_rsi))
    if min_rsi < 40:
        uptrend(ticker, min_rsi)


def algo(ticker):
    # while (len(database.bars[ticker][database.bars[ticker]['close'].isnull()]) > 0) or (len(database.bars[ticker]) != database.NUM_BARS):
    while len(database.bars[ticker]) != database.NUM_BARS:
        database.update_bars(ticker)

    print('Finished initialization for {0}'.format(ticker))

    while True:
        database.update_bars(ticker)

        b_bands = database.bollinger_bands(ticker)
        lower = b_bands[bounds['lower']].iloc[-1]

        if database.get_current_price(ticker) < lower:
            downtrend(ticker)


algo_threads = []
for ticker in tickers:
    algo_thread = Thread(target=algo, args=(ticker,))
    algo_thread.start()
    algo_threads.append(algo_thread)
