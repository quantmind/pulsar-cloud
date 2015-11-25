import unittest
import asyncio

from functools import wraps
from cloud.wrapper import AsyncBotocore
from pulsar.apps.http import HttpClient


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 150, loop=loop))
        return ret

    return wrapper


class BaseTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.doCleanups()
        self.loop._default_executor.shutdown(wait=True)
        self.loop.close()
        del self.loop


class AsyncBotocoreTest(BaseTest):
    def setUp(self):
        super().setUp()

        self.ec2 = AsyncBotocore('ec2', 'us-east-1',
                                 http_client=HttpClient(),
                                 loop=self.loop)
        self.addCleanup(self.ec2.close())

    @run_until_complete
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @run_until_complete
    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
