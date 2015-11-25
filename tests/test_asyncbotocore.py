import unittest
import asyncio

from cloud.wrapper import AsyncBotocore
from pulsar.apps.http import HttpClient


class AsyncBotocoreTest(unittest.TestCase):
    def setUp(self):
        http = HttpClient()
        self.ec2 = AsyncBotocore('ec2', 'us-east-1', http_client=http,
                                 loop=http._loop)

    @asyncio.coroutine
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @asyncio.coroutine
    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
