import unittest

from pulsar import task
from cloud.wrapper import PulsarBotocore


class PulsarBotocoreTest(unittest.TestCase):
    def setUp(self):
        self.ec2 = PulsarBotocore('ec2', 'us-east-1')
        self.s3 = PulsarBotocore('s3', 'us-east-1')

    @task
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @task
    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
