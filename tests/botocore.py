import unittest
import string

from botocore.exceptions import ClientError

from pulsar.utils.string import random_string
from pulsar.apps.greenio import GreenPool

from cloud import Botocore


def green(f):

    def _(self):
        return self.green_pool.submit(f, self)

    return _


class BotocoreTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.green_pool = GreenPool()
        cls.ec2 = Botocore('ec2', 'us-east-1', green_pool=cls.green_pool)
        cls.s3 = Botocore('s3', green_pool=cls.green_pool)

    def assert_status(self, response, code=200):
        meta = response['ResponseMetadata']
        self.assertEqual(meta['HTTPStatusCode'], code)

    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertTrue(response)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertTrue(response)

    def test_list_buckets(self):
        buckets = yield from self.s3.list_buckets()
        self.assertTrue(buckets)

    @green
    def test_get_object(self):
        response = self.s3.get_object(Bucket='quantmind-tests',
                                      Key='requirements.txt')
        meta = response['ResponseMetadata']
        self.assertEqual(meta['HTTPStatusCode'], 200)
        text = response['Body'].read()
        self.assertTrue(text)

    @green
    def test_upload_object(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = self.s3.put_object(Bucket='quantmind-tests',
                                          Body=body,
                                          ContentType='text/plain',
                                          Key=key)
            self.assert_status(response)
        #
        # Read object
        response = self.s3.get_object(Bucket='quantmind-tests',
                                      Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentType'], 'text/plain')
        #
        # Delete object
        response = self.s3.delete_object(Bucket='quantmind-tests',
                                         Key=key)
        self.assert_status(response, 204)
        self.assertRaises(ClientError, self.s3.get_object,
                          Bucket='quantmind-tests', Key=key)
