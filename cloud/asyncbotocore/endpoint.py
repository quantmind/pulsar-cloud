import os
import asyncio
import aiohttp

import botocore.endpoint
from botocore.endpoint import get_environ_proxies, DEFAULT_TIMEOUT
from botocore.exceptions import EndpointConnectionError


def _get_verify_value(verify):
    if verify is not None:
        return verify
    return os.environ.get('REQUESTS_CA_BUNDLE', True)


def text_(s, encoding='utf-8', errors='strict'):
    if isinstance(s, bytes):
        return s.decode(encoding, errors)
    return s  # pragma: no cover


def convert_to_response_dict(http_response, operation_model):
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status_code,
        'body': http_response.get_content()
    }
    return response_dict


class AsyncEndpoint(botocore.endpoint.Endpoint):
    def __init__(self, host,
                 endpoint_prefix, event_emitter, proxies=None, verify=True,
                 timeout=DEFAULT_TIMEOUT, response_parser_factory=None,
                 loop=None, http_client=None):
        super().__init__(host, endpoint_prefix,
                         event_emitter, proxies=proxies, verify=verify,
                         timeout=timeout,
                         response_parser_factory=response_parser_factory)

        self._loop = loop or asyncio.get_event_loop()
        self.http_client = http_client
        #self._connector = aiohttp.TCPConnector(loop=self._loop)

    @asyncio.coroutine
    def _request(self, method, url, headers, data):
        headers_ = dict(
            (z[0], text_(z[1], encoding='utf-8')) for z in headers.items())
        resp = yield from self.http_client.request(method=method, url=url,
                                                   data=data,
                                                   loop=self._loop,
                                                   headers=headers_,
                                                   connector=self._connector)
        return resp

    @asyncio.coroutine
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

        success_response, exception = yield from self._get_response(
            request, operation_model, attempts)

        if exception is not None:
            raise exception

        return success_response

    @asyncio.coroutine
    def _get_response(self, request, operation_model, attempts):
        try:
            resp = yield from self._request(
                request.method, request.url, request.headers, request.body)
            http_response = resp

        except ConnectionError as e:
            if self._looks_like_dns_error(e):
                endpoint_url = request.url
                better_exception = EndpointConnectionError(
                    endpoint_url=endpoint_url, error=e)
                return (None, better_exception)
            else:
                return (None, e)
        except Exception as e:
            return (None, e)

        response_dict = convert_to_response_dict(
            http_response, operation_model)
        parser = self._response_parser_factory.create_parser(
            operation_model.metadata['protocol'])
        return ((http_response, parser.parse(response_dict,
                                             operation_model.output_shape)),
                None)


class AsyncEndpointCreator(botocore.endpoint.EndpointCreator):
    def __init__(self, endpoint_resolver, configured_region, event_emitter,
                 user_agent, loop, http_client):
        super().__init__(endpoint_resolver, configured_region, event_emitter)
        self._loop = loop
        self.http_client = http_client

    def _get_endpoint(self, service_model, endpoint_url,
                      verify, response_parser_factory):
        endpoint_prefix = service_model.endpoint_prefix
        event_emitter = self._event_emitter
        return get_endpoint_complex(endpoint_prefix, endpoint_url, verify,
                                    event_emitter, response_parser_factory,
                                    loop=self._loop,
                                    http_client=self.http_client)


def get_endpoint_complex(endpoint_prefix,
                         endpoint_url, verify,
                         event_emitter,
                         response_parser_factory=None, loop=None,
                         http_client=None):
    proxies = get_environ_proxies(endpoint_url)
    verify = _get_verify_value(verify)
    return AsyncEndpoint(
        endpoint_url,
        endpoint_prefix=endpoint_prefix,
        event_emitter=event_emitter,
        proxies=proxies,
        verify=verify,
        response_parser_factory=response_parser_factory,
        loop=loop, http_client=http_client)
