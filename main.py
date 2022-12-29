#!/usr/bin/python3
from database import Database
import math


if __name__ == '__main__':
    ticker_file = open('./tickers')
    tickers = [ticker.strip() for ticker in ticker_file.readlines()]

    database = Database(tickers, 64)
    database.reset_trades()

    # tickers = ['AAPL']

    def algo(ticker):
        while len(database.bars[ticker]) != database.NUM_BARS:
            database.update_bars(ticker)
            # print(database.bars[ticker])

        while True:
            database.update_bars(ticker)
            # print(database.bars[ticker])
            # print('{0}: {1}'.format(ticker, database.calculate_rsi(ticker)))
            # print('{0}: {1}'.format(ticker, database.calculate_macd(ticker)))
            rsi = database.calculate_rsi(ticker)
            macd = database.calculate_macd(ticker)

            # check if past three bars are negative? maybe
            if not (math.isnan(rsi) or math.isnan(macd)):
                if rsi > 50 and macd > 0.01:
                    buy_price = database.get_current_price(ticker)
                    print('Bought {0} at {1}'.format(ticker, buy_price))
                    while True:
                        current_price = database.get_current_price(ticker)
                        if current_price > (1.001 * buy_price) or current_price < (0.999 * buy_price):
                            print('Sold {0} at {1} for a {2} of {3}'.format(ticker, current_price, 'profit' if current_price > buy_price else 'loss', abs(current_price - buy_price)))
                            database.send_trade(ticker, current_price - buy_price)
                            break

    algo_threads = []
    for ticker in tickers:
        algo_thread = Thread(target=algo, args=(ticker,))
        algo_thread.start()
        algo_threads.append(algo_thread)
