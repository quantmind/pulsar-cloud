import unittest
import asyncio
import os
import tempfile
import string

from pulsar.apps.http import HttpClient
from pulsar.utils.string import random_string

from cloud.asyncbotocore.session import get_session

ONEKB = 2**10
BUCKET = os.environ.get('TEST_S3_BUCKET', 'quantmind-tests')


class RandomFile:
    filename = None

    def __init__(self, size=ONEKB):
        self.size = size

    @property
    def key(self):
        if self.filename:
            return os.path.basename(self.filename)

    def __enter__(self):
        self.filename = tempfile.mktemp()
        with open(self.filename, 'wb') as fout:
            fout.write(os.urandom(self.size))
        return self

    def __exit__(self, *args):
        if self.filename:
            try:
                os.remove(self.filename)
            except FileNotFoundError:
                pass
            self.filename = None

    def body(self):
        if self.filename:
            with open(self.filename, 'rb') as f:
                return f.read()
        return b''


class AsyncBotocoreTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.http = HttpClient()
        cls.session = get_session(loop=cls.http._loop)
        cls.ec2 = cls.session.create_client('ec2',
                                            region_name='us-east-1',
                                            http_client=cls.http)
        cls.s3 = cls.session.create_client('s3',
                                           region_name='us-east-1',
                                           http_client=cls.http)

    @asyncio.coroutine
    def assert_status(self, response, status_code):
        self.assertEqual(
            response['ResponseMetadata']['HTTPStatusCode'],
            status_code
        )

    @asyncio.coroutine
    def assert_s3_equal(self, filename, copy_name):
        response = yield from self.s3.get_object(Bucket=BUCKET, Key=copy_name)
        with open(filename, 'rb') as fo:
            body = b''.join(self.s3.wsgi_stream_body(response['Body']))
            self.assertEqual(body, fo.read())

    @asyncio.coroutine
    def clean_up(self, key, size):
        response = self.s3.head_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 200)
        self.assertEqual(response['ContentLength'], size)
        # Delete
        response = yield from self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 204)

    @asyncio.coroutine
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assert_status(response, 200)

    @asyncio.coroutine
    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assert_status(response, 200)

    @asyncio.coroutine
    def test_upload_text(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = yield from (self.s3.put_object(Bucket=BUCKET, Body=body,
                                                      ContentType='text/plain',
                                                      Key=key))
            self.assert_status(response, 200)

        # Read object
        yield from asyncio.sleep(3, loop=self.http._loop)
        response = yield from self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 200)
        self.assertEqual(response['ContentType'], 'text/plain')

        # Delete Object
        yield from asyncio.sleep(3, loop=self.http._loop)
        response = yield from self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 204)
