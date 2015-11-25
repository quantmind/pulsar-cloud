import unittest
import asyncio
import os
import tempfile
import string

from botocore.exceptions import ClientError

from cloud.wrapper import AsyncBotocore
from pulsar.apps.http import HttpClient
from pulsar.utils.string import random_string


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
    def setUp(self):
        http = HttpClient()
        self.ec2 = AsyncBotocore('ec2', 'us-east-1', http_client=http,
                                 loop=http._loop)
        self.s3 = AsyncBotocore('s3', 'us-east-1', http_client=http,
                                loop=http._loop)

    @asyncio.coroutine
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @asyncio.coroutine
    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @asyncio.coroutine
    def test_upload_text(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = yield from (self.s3.put_object(Bucket=BUCKET, Body=body,
                                                      ContentType='text/plain',
                                                      Key=key))
            self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'],
                             200)
        # Read object
        response = yield from (self.s3.get_object(Bucket=BUCKET, Key=key))
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(response['ContentType'], 'text/plain')

        # Delete Object
        response = yield from self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 204)
        self.assertRaises(ClientError, self.s3.get_object,
                          Bucket=BUCKET, Key=key)
