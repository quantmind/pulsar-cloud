import unittest
import string

from pulsar.apps.http import HttpClient
from pulsar.utils.string import random_string

from cloud.asyncbotocore.session import get_session

from . import BotoMixin, RandomFile, BUCKET, ONEKB


class AsyncBotocoreTest(unittest.TestCase, BotoMixin):

    @classmethod
    def setUpClass(cls):
        cls.http = HttpClient()
        cls.session = get_session()
        cls.ec2 = cls.session.create_client('ec2',
                                            region_name='us-east-1',
                                            http_session=cls.http)
        cls.s3 = cls.session.create_client('s3',
                                           region_name='us-east-1',
                                           http_session=cls.http)

    def assert_s3_equal(self, filename, copy_name):
        response = yield from self.s3.get_object(Bucket=BUCKET, Key=copy_name)
        with open(filename, 'rb') as fo:
            body = b''.join(self.s3.wsgi_stream_body(response['Body']))
            self.assertEqual(body, fo.read())

    def clean_up(self, key, size):
        response = self.s3.head_object(Bucket=BUCKET, Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentLength'], size)
        # Delete
        response = yield from self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 204)

    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assert_status(response)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assert_status(response)

    def test_upload_text(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = yield from self.s3.put_object(Bucket=BUCKET, Body=body,
                                                     ContentType='text/plain',
                                                     Key=key)
            self.assert_status(response)

        # Read object
        response = yield from self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentType'], 'text/plain')

        # Delete Object
        response = yield from self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 204)

    def test_upload_bytes(self):
        with RandomFile(ONEKB) as r:
            response = yield from self.s3.put_object(Bucket=BUCKET,
                                                     Body=r.body(),
                                                     Key=r.key)
            self.assert_status(response)
            response = yield from self.s3.get_object(Bucket=BUCKET, Key=r.key)
            self.assert_status(response)
