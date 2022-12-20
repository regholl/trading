#!/usr/bin/python3
import psycopg2 as pg

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as dates

import warnings
import os

from dotenv import dotenv_values

config = {
    **dotenv_values(),
    **os.environ
}
warnings.filterwarnings('ignore')

engine = pg.connect(
    "user='{0}' password='{1}' host='{2}' port='{3}'".format(
        config['DB_USER'],
        config['DB_PASSWORD'],
        config['DB_HOST'],
        config['DB_PORT']
    )
)
df = pd.read_sql('select * from SPY', con=engine)

x = df.loc[:,"timestamp"].to_numpy()
y = df.loc[:,"LAST_PRICE"].to_numpy()
x = np.array(x, dtype='datetime64[ns]')

plt.plot(x, y)

plt.show()
