from pulsar.apps.http import HttpClient
from pulsar.apps.greenio import wait

from .asyncbotocore import get_session
from .utils.s3 import S3tools


class AsyncioBotocore(S3tools):
    '''High level Asynchornous botocore wrapper
    '''
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, loop=None,
                 session=None, http_session=None,
                 **kwargs):
        if not http_session:
            http_session = HttpClient(loop=loop)
        self._session = get_session()
        self._client = self._session.create_client(
            service_name, region_name=region_name,
            endpoint_url=endpoint_url,
            http_session=http_session,
            **kwargs)

    @property
    def _loop(self):
        return self._client._loop

    @property
    def endpoint(self):
        return self._client._endpoint

    @property
    def http_session(self):
        '''HTTP session object
        '''
        return self.endpoint.http_session

    def __getattr__(self, operation):
        return getattr(self._client, operation)


class GreenProxy:

    def __init__(self, client):
        self.client = client

    def __getattr__(self, operation):
        return GreenApiCall(operation, self.client)


class GreenBotocore(GreenProxy):
    '''High level Asynchornous botocore wrapper for greenlet
    '''
    def __init__(self, service_name, **kwargs):
        super().__init__(AsyncioBotocore(service_name, **kwargs))

    @property
    def http_session(self):
        '''HTTP session object
        '''
        return self.client.http_session

    def get_paginator(self, operation_name):
        return GreenPaginator(self.client.get_paginator(operation_name))


class GreenIterator(GreenProxy):
    '''A pulsar compliant WSGI iterator
    '''
    def __iter__(self):
        for data in self.client:
            yield wait(data)


class GreenPaginator(GreenProxy):

    def paginate(self, *args, **kwargs):
        return GreenIterator(self.client.paginate(*args, **kwargs))


class GreenApiCall:

    def __init__(self, operation, client):
        self.operation = operation
        self.client = client

    def __repr__(self):
        return self.operation
    __str__ = __repr__

    def __call__(self, *args, **kwargs):
        return wait(self._wrap_body(args, kwargs))

    async def _wrap_body(self, args, kwargs):
        result = await getattr(self.client, self.operation)(*args, **kwargs)
        if isinstance(result, dict):
            body = result.get('Body')
            if body is not None and not isinstance(body, bytes):
                result['Body'] = GreenIterator(body)
        return result
