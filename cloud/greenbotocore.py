from functools import wraps

import botocore.session

from pulsar.apps.greenio import GreenPool

from .asyncbotocore.session import get_session
from .s3 import S3tools


def green(f):

    @wraps(f)
    def _(self, *args):
        return self._green_pool.wait(f(self, *args))

    return _


class GreenBotocore(S3tools):

    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, loop=None, green_pool=None,
                 session=None, http_client=None,
                 **kwargs):
        self._green_pool = green_pool or GreenPool(loop=loop)

        # Use asynchronous boto
        if http_client:
            _get_session = get_session
            self._blocking_call = self._asyncio_call
            kwargs['http_session'] = http_client
        else:
            _get_session = botocore.session.get_session
            self._blocking_call = self._thread_call

        self.session = session or _get_session()
        self.client = self.session.create_client(
            service_name, region_name=region_name,
            endpoint_url=endpoint_url,
            **kwargs)
        self._botocore_api_call = self.client._make_api_call
        self.client._make_api_call = self._make_api_call

    @property
    def _loop(self):
        return self._green_pool._loop

    @property
    def concurrency(self):
        if self._blocking_call == self._asyncio_call:
            return 'asyncio'
        else:
            return 'thread'

    def green_pool(self):
        return self._green_pool

    def wsgi_stream_body(self, body, n=-1):
        '''WSGI iterator of a botocore StreamingBody
        '''
        return GreenWsgiIterator(body, self._blocking_call, n)

    def __getattr__(self, operation):
        return getattr(self.client, operation)

    # INTERNALS
    def _make_api_call(self, operation, kwargs):
        return self._blocking_call(self._botocore_api_call, operation, kwargs)

    @green
    def _asyncio_call(self, func, *args):
        '''A call using the asyncio botocore.
        '''
        return func(*args)

    @green
    def _thread_call(self, func, *args):
        '''A call using the event loop executor.
        '''
        return self._loop.run_in_executor(None, func, *args)


class GreenWsgiIterator:
    '''A pulsar compliant WSGI iterator
    '''
    _data = None

    def __init__(self, body, blocking_call, n=-1):
        self.body = body
        self._blocking_call = blocking_call
        self.n = n

    def __iter__(self):

        while True:
            yield self._blocking_call(self._read_body)
            if not self._data:
                break

    def _read_body(self):
        self._data = self.body.read(self.n)
        return self._data
