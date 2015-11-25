import unittest

from cloud.wrapper import AsyncBotocore
from pulsar.apps.http import HttpClient


class AsyncBotocoreTest(unittest.TestCase):
    def setUp(self):
        self.ec2 = AsyncBotocore('ec2', 'us-east-1', http_client=HttpClient())
        self.s3 = AsyncBotocore('s3', 'us-east-1', http_client=HttpClient())

    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
