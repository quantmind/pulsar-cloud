import os
import asyncio
import logging

import botocore.endpoint
from botocore.endpoint import get_environ_proxies
from botocore.exceptions import EndpointConnectionError, \
    BaseEndpointResolverError
from botocore.utils import is_valid_endpoint_url
from botocore.awsrequest import create_request_object

logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60


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

    @asyncio.coroutine
    def create_request(self, params, operation_model=None):
        request = create_request_object(params)
        if operation_model:
            event_name = 'request-created.{endpoint_prefix}.{op_name}'.format(
                endpoint_prefix=self._endpoint_prefix,
                op_name=operation_model.name)
            yield from self._event_emitter.emit(
                event_name, request=request,
                operation_name=operation_model.name)
        prepared_request = self.prepare_request(request)
        return prepared_request

    @asyncio.coroutine
    def _request(self, method, url, headers, data):
        headers_ = dict(
            (z[0], text_(z[1], encoding='utf-8')) for z in headers.items())
        resp = yield from self.http_client.request(method=method, url=url,
                                                   data=data,
                                                   loop=self._loop,
                                                   headers=headers_)
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
        request = yield from self.create_request(request_dict, operation_model)

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

    @asyncio.coroutine
    def make_request(self, operation_model, request_dict):
        logger.debug("Making request for %s (verify_ssl=%s) with params: %s",
                     operation_model, self.verify, request_dict)
        return (yield from self._send_request(request_dict, operation_model))


class AsyncEndpointCreator(botocore.endpoint.EndpointCreator):
    def __init__(self, endpoint_resolver, configured_region, event_emitter,
                 user_agent, loop, http_client):
        super().__init__(endpoint_resolver, configured_region, event_emitter)
        self._loop = loop
        self.http_client = http_client

    def create_endpoint(self, service_model, region_name=None, is_secure=True,
                        endpoint_url=None, verify=None,
                        response_parser_factory=None, timeout=DEFAULT_TIMEOUT):
        if region_name is None:
            region_name = self._configured_region
        # Use the endpoint resolver heuristics to build the endpoint url.
        scheme = 'https' if is_secure else 'http'
        try:
            endpoint = self._endpoint_resolver.construct_endpoint(
                service_model.endpoint_prefix,
                region_name, scheme=scheme)
        except BaseEndpointResolverError:
            if endpoint_url is not None:
                # If the user provides an endpoint_url, it's ok
                # if the heuristics didn't find anything.  We use the
                # user provided endpoint_url.
                endpoint = {'uri': endpoint_url, 'properties': {}}
            else:
                raise

        if endpoint_url is not None:
            # If the user provides an endpoint url, we'll use that
            # instead of what the heuristics rule gives us.
            final_endpoint_url = endpoint_url
        else:
            final_endpoint_url = endpoint['uri']
        if not is_valid_endpoint_url(final_endpoint_url):
            raise ValueError("Invalid endpoint: %s" % final_endpoint_url)
        return self._get_endpoint(
            service_model, final_endpoint_url, verify, response_parser_factory)

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
