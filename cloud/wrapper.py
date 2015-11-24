from pulsar_botocore.session import get_session
from .sock import wrap_poolmanager


class PulsarBotocore(object):
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, session=None, **kwargs):
        self.session = session or get_session()
        self.client = self.session.create_client(
            service_name, region_name=region_name, endpoint_url=endpoint_url,
            **kwargs)

        endpoint = self.client._endpoint
        for adapter in endpoint.http_session.adapters.values():
            adapter.poolmanager = wrap_poolmanager(adapter.poolmanager)

    def __getattr__(self, operation):
        return getattr(self.client, operation)
