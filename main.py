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


def uptrend(ticker, lower_bound=None, upper_bound=None):
    print('Starting uptrend on {0} from {1} to {2}'.format(ticker, lower_bound, upper_bound))
    holding = False
    buy_price = 0
    num_shares = 0

    lower_bound = bounds[lower_bound] if lower_bound is not None else 'lower'
    upper_bound = bounds[upper_bound] if upper_bound is not None else 'upper'

    database.update_bars(ticker)
    b_bands = database.bollinger_bands(ticker)

    # add logic for price levels given purchase history
    while (database.get_current_price(ticker) > b_bands[lower_bound].iloc[-2]) and (database.get_current_price(ticker) < b_bands[upper_bound].iloc[-2]):
        holding = True
        buy_price = database.get_current_price(ticker)
        num_shares = math.ceil(1000 / buy_price)
        buy_time = pd.Timestamp.now()
        print('Bought {0} shares of {1} at {2}'.format(num_shares, ticker, buy_price))

        while (database.get_current_price(ticker) > b_bands[lower_bound].iloc[-2]) and (database.get_current_price(ticker) < b_bands[upper_bound].iloc[-2]):
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

    exit_type = -1 if database.get_current_price(ticker) < b_bands[lower_bound].iloc[-2] else 1

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


def downtrend(ticker):
    print('Starting downtrend on {0}'.format(ticker))
    while database.get_current_price(ticker) < database.bollinger_bands(ticker)[bounds['lower']].iloc[-2]:
        database.update_bars(ticker)

    if database.rsi(ticker) < 40:
        # uptrend with middle as upper limit and low as lower limit
        crossed = False if uptrend(ticker, 'lower', 'mid') < 0 else True

        # start an uptrend that lasts until crossing upper band or dipping below middle band
        if crossed:
            uptrend(ticker, 'mid', 'upper')


def algo(ticker):
    while len(database.bars[ticker]) != database.NUM_BARS:
        database.update_bars(ticker)

    print('Finished initialization for {0}'.format(ticker))
    # print(database.bars[ticker])

    while True:
        database.update_bars(ticker)

        b_bands = database.bollinger_bands(ticker)
        lower = b_bands[bounds['lower']].iloc[-2]
        upper = b_bands[bounds['upper']].iloc[-2]

        # if database.get_current_price(ticker) > upper:
        #     uptrend(ticker, lower_bound='upper')
        if database.get_current_price(ticker) < lower:
            downtrend(ticker)


algo_threads = []
for ticker in tickers:
    algo_thread = Thread(target=algo, args=(ticker,))
    algo_thread.start()
    algo_threads.append(algo_thread)
