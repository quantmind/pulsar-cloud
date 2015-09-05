import unittest

from cloud import Botocore


class BotocoreTest(unittest.TestCase):

    def test_botocore(self):
        ec2 = Botocore('ec2', 'us-east-1')
        response = yield from ec2.call('DescribeInstances')
        self.assertTrue(response)
