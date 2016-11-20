import os
import tempfile
import unittest

from pulsar.apps.greenio import GreenPool
from pulsar.apps.http import HttpClient


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


def green(f):

    def _(self):
        return self.green_pool.submit(f, self)

    return _


class BotocoreMixin:
    pool_size = 10

    @classmethod
    def setUpClass(cls):
        cls.green_pool = GreenPool()
        cls.sessions = HttpClient(pool_size=cls.pool_size,
                                  decompress=False)
        cls.kwargs = dict(http_session=cls.sessions,
                          region_name='us-east-1')
        cls.test = unittest.TestCase()

    @classmethod
    def tearDownClass(cls):
        return cls.sessions.close()

    @classmethod
    def assert_status(cls, response, code=200):
        meta = response['ResponseMetadata']
        cls.test.assertEqual(meta['HTTPStatusCode'], code)
