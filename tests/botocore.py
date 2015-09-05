import unittest

from cloud import Botocore


class BotocoreTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ec2 = Botocore('ec2', 'us-east-1')
        cls.s3 = Botocore('s3')

    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertTrue(response)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertTrue(response)

    def test_list_buckets(self):
        buckets = yield from self.s3.list_buckets()
        self.assertTrue(buckets)
