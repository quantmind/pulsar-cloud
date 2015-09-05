import unittest

from cloud import Botocore


class BotocoreTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ec2 = Botocore('ec2', 'us-east-1')

    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertTrue(response)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertTrue(response)
