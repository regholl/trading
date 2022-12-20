#!/usr/bin/python3
import asyncio
from datetime import datetime
import sys

from tda.auth import client_from_token_file
from tda.client import Client
from tda.streaming import StreamClient

from questdb.ingress import Sender, IngressError

import os
from dotenv import dotenv_values

config = {
    **dotenv_values(),
    **os.environ
}

API_KEY = config["TDA_CLIENT_KEY"]
ACCOUNT_ID = config["TDA_ACCOUNT_ID"]
TOKEN_PATH = config["TOKEN_PATH"]
DB_HOST = config["DB_HOST"]
DB_PORT = config["DB_INSERT_PORT"]
TICKER_PATH = config["TICKER_PATH"]

tickers = []
ticker_file = open(TICKER_PATH)
ticker_lines = ticker_file.readlines()
ticker_file.close()

for ticker in ticker_lines:
    tickers.append(ticker.strip())

class Stream:
    def __init__(self, api_key, account_id, queue_size=0,
                 credentials_path=TOKEN_PATH):
        self.api_key = api_key
        self.account_id = account_id
        self.credentials_path = credentials_path
        self.tda_client = None
        self.stream_client = None
        self.symbols = tickers
        self.collections = {}

        self.queue = asyncio.Queue(queue_size)

    def initialize(self):
        self.tda_client = client_from_token_file(
            api_key=self.api_key,
            token_path=self.credentials_path)
        self.stream_client = StreamClient(
            self.tda_client, account_id=self.account_id)

        self.stream_client.add_level_one_equity_handler(
            self.handle_quotes)

    async def stream(self):
        await self.stream_client.login()
        await self.stream_client.quality_of_service(StreamClient.QOSLevel.EXPRESS)
        await self.stream_client.level_one_equity_subs(symbols=self.symbols, fields=[
            self.stream_client.LevelOneEquityFields.ASK_PRICE,
            self.stream_client.LevelOneEquityFields.ASK_SIZE,
            self.stream_client.LevelOneEquityFields.BID_PRICE,
            self.stream_client.LevelOneEquityFields.BID_SIZE,
            self.stream_client.LevelOneEquityFields.LAST_PRICE,
            self.stream_client.LevelOneEquityFields.LAST_SIZE,
            self.stream_client.LevelOneEquityFields.MARK,
            self.stream_client.LevelOneEquityFields.TOTAL_VOLUME,
            ])

        asyncio.ensure_future(self.handle_queue())

        while True:
            await self.stream_client.handle_message()

    async def handle_quotes(self, msg):
        if self.queue.full():  # This won't happen if the queue doesn't have a max size
            print('Handler queue is full. Awaiting to make room... Some messages might be dropped')
            await self.queue.get()
        await self.queue.put(msg)

    async def handle_queue(self):
        while True:
            msg = await self.queue.get()
            for message in msg['content']:
                if "LAST_PRICE" in message:
                    key = message['key']
                    if 'key' in message:
                        del message['key']
                    if 'delayed' in message:
                        del message['delayed']
                    try:
                        with Sender(DB_HOST, DB_PORT) as sender:
                            sender.row(
                                key,
                                columns=message
                            )
                            sender.flush()
                            print(message)
                    except IngressError as e:
                        sys.stderr.write(f'Got error: {e}\n')


async def main():
    consumer = Stream(API_KEY, ACCOUNT_ID)
    consumer.initialize()
    await consumer.stream()

if __name__ == '__main__':
    asyncio.run(main())
