import botocore.session

from pulsar.apps.greenio import GreenPool, getcurrent
from .sock import wrap_poolmanager


class Botocore(object):
    '''An asynchronous wrapper for botocore
    '''
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, session=None, **kwargs):
        self._green_pool = None
        self.session = session or botocore.session.get_session()
        self.client = self.session.create_client(service_name,
                                                 region_name=region_name,
                                                 endpoint_url=endpoint_url,
                                                 **kwargs)
        endpoint = self.client._endpoint
        for adapter in endpoint.http_session.adapters.values():
            adapter.poolmanager = wrap_poolmanager(adapter.poolmanager)

        self._make_api_call = self.client._make_api_call
        self.client._make_api_call = self._call

    def __getattr__(self, operation):
        return getattr(self.client, operation)

    def green_pool(self):
        if not self._green_pool:
            self._green_pool = GreenPool()
        return self._green_pool

    def _call(self, operation, kwargs):
        if getcurrent().parent:
            return self._make_api_call(operation, kwargs)
        else:
            pool = self.green_pool()
            return pool.submit(self._make_api_call, operation, kwargs)
