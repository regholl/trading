#!/usr/bin/env python3
import psycopg2 as pg

import pandas as pd
import numpy as np

import warnings
import os

from dotenv import dotenv_values


class Database:

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
