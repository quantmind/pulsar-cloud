import os
import botocore.endpoint
from botocore.endpoint import get_environ_proxies, DEFAULT_TIMEOUT
from pulsar import get_event_loop, new_event_loop, TcpServer


def _get_verify_value(verify):
    if verify is not None:
        return verify
    return os.environ.get('REQUESTS_CA_BUNDLE', True)


class PulsarEndpoint(botocore.endpoint.Endpoint):
    def __init__(self, host,
                 endpoint_prefix, event_emitter, proxies=None, verify=True,
                 timeout=DEFAULT_TIMEOUT, response_parser_factory=None,
                 loop=None):
        super().__init__(host, endpoint_prefix,
                         event_emitter, proxies=proxies, verify=verify,
                         timeout=timeout,
                         response_parser_factory=response_parser_factory)

        self._loop = loop or get_event_loop() or new_event_loop()
        self._connector = TcpServer(protocol_factory=None, loop=self._loop)

        # TODO


class PulsarEndpointCreator(botocore.endpoint.EndpointCreator):
    def __init__(self, endpoint_resolver, configured_region, event_emitter,
                 user_agent, loop):
        super().__init__(endpoint_resolver, configured_region, event_emitter)
        self._loop = loop

    def _get_endpoint(self, service_model, endpoint_url,
                      verify, response_parser_factory):
        endpoint_prefix = service_model.endpoint_prefix
        event_emitter = self._event_emitter
        return get_endpoint_complex(endpoint_prefix, endpoint_url, verify,
                                    event_emitter, response_parser_factory,
                                    loop=self._loop)


def get_endpoint_complex(endpoint_prefix,
                         endpoint_url, verify,
                         event_emitter,
                         response_parser_factory=None, loop=None):
    proxies = get_environ_proxies(endpoint_url)
    verify = _get_verify_value(verify)
    return PulsarEndpoint(
        endpoint_url,
        endpoint_prefix=endpoint_prefix,
        event_emitter=event_emitter,
        proxies=proxies,
        verify=verify,
        response_parser_factory=response_parser_factory,
        loop=loop)
