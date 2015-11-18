import os
import unittest
import string
import tempfile

from botocore.exceptions import ClientError

from pulsar.utils.string import random_string
from pulsar.apps.greenio import GreenPool

from cloud import Botocore
from cloud.pulsar_botocore import MULTI_PART_SIZE


def green(f):

    def _(self):
        return self.green_pool.submit(f, self)

    return _


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


class BotocoreMixin:
    _green = True

    @classmethod
    def setUpClass(cls):
        cls.green_pool = GreenPool()
        cls.ec2 = Botocore('ec2', 'us-east-1', green_pool=cls.green_pool,
                           green=cls._green)
        cls.s3 = Botocore('s3', green_pool=cls.green_pool, green=cls._green)

    def assert_status(self, response, code=200):
        meta = response['ResponseMetadata']
        self.assertEqual(meta['HTTPStatusCode'], code)

    def clean_up(self, key, size):
        response = self.s3.head_object(Bucket=BUCKET, Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentLength'], size)
        # Delete
        response = self.s3.delete_object(Bucket=BUCKET,
                                         Key=key)
        self.assert_status(response, 204)
        self.assertRaises(ClientError, self.s3.get_object,
                          Bucket=BUCKET, Key=key)

    # TESTS
    def test_describe_instances(self):
        response = yield from self.ec2.describe_instances()
        self.assertTrue(response)
        # https_adapter = self.ec2._endpoint.http_session.adapters['https://']
        # pools = list(https_adapter.poolmanager.pools.values())
        # self.assertEqual(len(pools), 1)

    def test_describe_spot_price_history(self):
        response = yield from self.ec2.describe_spot_price_history()
        self.assertTrue(response)

    @green
    def test_upload_text(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = self.s3.put_object(Bucket=BUCKET,
                                          Body=body,
                                          ContentType='text/plain',
                                          Key=key)
            self.assert_status(response)
        #
        # Read object
        response = self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentType'], 'text/plain')
        #
        # Delete object
        response = self.s3.delete_object(Bucket=BUCKET,
                                         Key=key)
        self.assert_status(response, 204)
        self.assertRaises(ClientError, self.s3.get_object,
                          Bucket=BUCKET, Key=key)

    @green
    def test_upload_binary(self):
        with RandomFile(2**12) as r:
            response = self.s3.upload_file(BUCKET, r.filename)
            self.assert_status(response)
            self.clean_up(r.key, r.size)

    @green
    def test_upload_binary_large(self):
        with RandomFile(int(1.5*MULTI_PART_SIZE)) as r:
            response = self.s3.upload_file(BUCKET,
                                           r.filename)
            self.assert_status(response)
            self.clean_up(r.key, r.size)

    @green
    def test_copy(self):
        self._test_copy(2**12)

    @green
    def test_copy_large(self):
        self._test_copy(1.5*MULTI_PART_SIZE)

    def _test_copy(self, size):
        with RandomFile(int(size)) as r:
            response = self.s3.upload_file(BUCKET, r.filename)
            self.assert_status(response)
            copy_key = 'copy_{}'.format(r.key)
            response = self.s3.copy_storage_object(
                BUCKET, r.key, BUCKET, copy_key)
            self.assert_status(response)
            self.assert_s3_equal(r.filename, copy_key)
            self.clean_up(r.key, r.size)
            self.clean_up(copy_key, r.size)

    def assert_s3_equal(self, filename, copy_name):
        response = self.s3.get_object(Bucket=BUCKET, Key=copy_name)
        with open(filename, 'rb') as fo:
            body = b''.join(self.s3.wsgi_stream_body(response['Body']))
            self.assertEqual(body, fo.read())


class BotocoreTestGreen(BotocoreMixin, unittest.TestCase):

    @green
    def test_concurrency(self):
        self.assertEqual(self.s3.concurrency, 'green')


class BotocoreTestThread(BotocoreMixin, unittest.TestCase):
    _green = False

    @green
    def test_concurrency(self):
        self.assertEqual(self.s3.concurrency, 'thread')
