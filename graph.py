#!/usr/bin/env python3
import plotly.graph_objects as go

from database import Database

stock = 'AMD'

tickers = [stock]
ticker = stock
database = Database(tickers, 64)

while len(database.bars[ticker]) != database.NUM_BARS:
    database.update_bars(ticker)

df = database.bars[ticker]
b_bands = database.bollinger_bands(ticker)
trades = database.get_trades(ticker)

fig = go.Figure(go.Candlestick(
    x=df.index,
    open=df['open'],
    high=df['high'],
    low=df['low'],
    close=df['close']
))
fig.add_trace(go.Line(x=b_bands.index, y=b_bands['BBL_20_2.0']))
fig.add_trace(go.Line(x=b_bands.index, y=b_bands['BBM_20_2.0']))
fig.add_trace(go.Line(x=b_bands.index, y=b_bands['BBU_20_2.0']))

for index, row in trades.iterrows():
    times = [row['buy_time'].floor('T'), row['sell_time'].floor('T')]
    values = [row['buy_price'], row['sell_price']]
    fig.add_trace(go.Line(x=times, y=values))

fig.update_layout(xaxis_rangeslider_visible=False)
fig.show()

while True:
    fig.update_layout(title_text="Hello")
