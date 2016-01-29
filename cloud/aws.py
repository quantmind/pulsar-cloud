from pulsar.apps.http import HttpClient
from pulsar.apps.greenio import wait

from .asyncbotocore.endpoint import StreamingBody
from .asyncbotocore.session import get_session
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


class GreenBotocore:
    '''High level Asynchornous botocore wrapper for greenlet
    '''
    def __init__(self, service_name, **kwargs):
        self._client = AsyncioBotocore(service_name, **kwargs)

    @property
    def _loop(self):
        return self._client._loop

    @property
    def http_session(self):
        '''HTTP session object
        '''
        return self._client.http_session

    def __getattr__(self, operation):
        return GreenApiCall(operation, self)


class GreenApiCall:

    def __init__(self, operation, green_client):
        self.operation = operation
        self.green_client = green_client

    def __repr__(self):
        return self.operation
    __str__ = __repr__

    def __call__(self, *args, **kwargs):
        return wait(self._wrap_body(args, kwargs))

    def _wrap_body(self, args, kwargs):
        client = self.green_client._client
        result = yield from getattr(client, self.operation)(*args, **kwargs)
        if isinstance(result, dict):
            body = result.get('Body')
            if isinstance(body, StreamingBody):
                result['Body'] = GreenBody(body)
        return result


class GreenBody:
    '''A pulsar compliant WSGI iterator
    '''
    def __init__(self, body):
        self.body = body

    def read(self):
        return wait(self.body.read())

    def __iter__(self):
        for data in self.body:
            yield wait(data)
