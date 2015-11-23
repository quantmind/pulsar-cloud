import botocore.endpoint
from botocore.endpoint import get_environ_proxies, DEFAULT_TIMEOUT
from pulsar import get_event_loop, new_event_loop, TcpServer, as_coroutine
from pulsar import task
from pulsar.apps.http import HttpClient
import os


def convert_to_response_dict(http_response, operation_model):
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status_code,
        'body': http_response.get_content()
    }
    return response_dict
    # TODO


def _get_verify_value(verify):
    if verify is not None:
        return verify
    return os.environ.get('REQUESTS_CA_BUNDLE', True)


def text_(s, encoding='utf-8', errors='strict'):
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    return s  # pragma: no cover


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
        self._client = HttpClient(loop=self._loop)

    @task
    def _request(self, method, url, headers, data):
        headers_ = dict(
            (z[0], text_(z[1], encoding='utf-8')) for z in headers.items())

        resp = yield from self._client.request(method=method, url=url,
                                               timeout=None, data=data,
                                               loop=self._loop,
                                               headers=headers_)
        return resp

    def _send_request(self, request_dict, operation_model):
        headers = request_dict['headers']
        for key in headers.keys():
            if key.lower().startswith('content-type'):
                break
        else:
            request_dict['headers']['Content-Type'] = \
                'application/octet-stream'

        attempts = 1

        request = self.create_request(request_dict, operation_model)

        success_response, exception = yield from as_coroutine(
            self._get_response(request, operation_model, attempts))

        if exception is not None:
            raise exception

        return success_response

    @task
    def _get_response(self, request, operation_model, attempts):
        try:
            resp = yield from self._request(
                request.method, request.url, request.headers, request.body)
            http_response = resp

        except ConnectionError as e:
            print(e)  # TODO

        response_dict = convert_to_response_dict(
            http_response, operation_model)
        parser = self._response_parser_factory.create_parser(
            operation_model.metadata['protocol'])
        return ((http_response, parser.parse(response_dict,
                                             operation_model.output_shape)),
                None)
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
