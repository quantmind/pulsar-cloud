import asyncio
import logging

import botocore.endpoint
from botocore.endpoint import first_non_none_response
from botocore.exceptions import (EndpointConnectionError,
                                 BaseEndpointResolverError)
from botocore.utils import is_valid_endpoint_url
from botocore.awsrequest import create_request_object
from botocore import response

logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60


def convert_to_response_dict(http_response, operation_model):
    headers = http_response.headers

    if not isinstance(headers, dict):
        headers = dict(headers)

    response_dict = {
        'headers': headers,
        'status_code': http_response.status_code,
    }
    if (response_dict['status_code'] < 300 and
            operation_model.has_streaming_output):
        cl = response_dict['headers'].get('content-length')
        body = StreamingBody(http_response.raw, cl)
    else:
        # TODO: we need a common API for waiting for body
        yield from http_response.on_finished
        body = http_response.content

    response_dict['body'] = body
    return response_dict


class StreamingBody(response.StreamingBody):

    def set_socket_timeout(self, timeout):
        pass

    def read(self, amt=None):
        return self._raw_stream.read()

    def __iter__(self):
        return iter(self._raw_stream)


class AsyncEndpoint(botocore.endpoint.Endpoint):
    '''Asynchronous endpoint based on asyncio.

    the ``http_session`` object is an asynchronous http client with
    and api similar to python requests
    '''
    def __init__(self, http_session, *args, **kw):
        super().__init__(*args, **kw)
        self.http_session = http_session

    @property
    def _loop(self):
        return self.http_session._loop

    @asyncio.coroutine
    def create_request(self, params, operation_model=None):
        request = create_request_object(params)
        if operation_model:
            event_name = 'request-created.{endpoint_prefix}.{op_name}'.format(
                endpoint_prefix=self._endpoint_prefix,
                op_name=operation_model.name)
            yield from self._event_emitter.emit(
                event_name,
                request=request,
                operation_name=operation_model.name)
        prepared_request = self.prepare_request(request)
        return prepared_request

    @asyncio.coroutine
    def _send_request(self, request_dict, operation_model):
        '''
        headers = request_dict['headers']
        for key in headers.keys():
            if key.lower().startswith('content-type'):
                break
        else:
            request_dict['headers']['Content-Type'] = \
                'application/octet-stream'
        '''
        attempts = 0
        retry = True

        while retry:
            attempts += 1
            request = yield from self.create_request(request_dict,
                                                     operation_model)

            success_response, exception = yield from self._get_response(
                request, operation_model, attempts)

            retry = yield from self._needs_retry(attempts, operation_model,
                                                 success_response, exception)

        if exception is not None:
            raise exception
        else:
            return success_response

    @asyncio.coroutine
    def _get_response(self, request, operation_model, attempts):
        try:
            headers = dict(self._headers(request.headers))
            http_response = yield from self.http_session.request(
                method=request.method, url=request.url, data=request.body,
                headers=headers, stream=True)
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

        response_dict = yield from convert_to_response_dict(
            http_response, operation_model)
        parser = self._response_parser_factory.create_parser(
            operation_model.metadata['protocol'])
        return ((http_response, parser.parse(response_dict,
                                             operation_model.output_shape)),
                None)

    def _needs_retry(self, attempts, operation_model, response=None,
                     caught_exception=None):
        event_name = 'needs-retry.%s.%s' % (self._endpoint_prefix,
                                            operation_model.name)
        responses = yield from self._event_emitter.emit(
            event_name, response=response, endpoint=self,
            operation=operation_model, attempts=attempts,
            caught_exception=caught_exception)
        handler_response = first_non_none_response(responses)
        if handler_response is None:
            return False
        else:
            # Request needs to be retried, and we need to sleep
            # for the specified number of times.
            logger.debug("Response received to retry, sleeping for "
                         "%s seconds", handler_response)
            yield from asyncio.sleep(handler_response)
            return True

    def _headers(self, headers):
        for k, v in headers.items():
            if isinstance(v, bytes):
                v = v.decode('utf-8')
            yield k, v


class AsyncEndpointCreator(botocore.endpoint.EndpointCreator):

    def __init__(self, http_session, *args):
        super().__init__(*args)
        self.http_session = http_session

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

        proxies = self._get_proxies(final_endpoint_url)
        verify_value = self._get_verify_value(verify)
        return AsyncEndpoint(
            self.http_session,
            final_endpoint_url,
            endpoint_prefix=service_model.endpoint_prefix,
            event_emitter=self._event_emitter,
            proxies=proxies,
            verify=verify_value,
            timeout=timeout,
            response_parser_factory=response_parser_factory)
