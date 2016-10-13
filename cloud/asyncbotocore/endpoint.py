import asyncio
import logging

import botocore.endpoint
from botocore.endpoint import first_non_none_response, MAX_POOL_CONNECTIONS
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
from botocore.utils import is_valid_endpoint_url


logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 60


async def convert_to_response_dict(http_response, operation_model):
    headers = http_response.headers

    if not isinstance(headers, dict):
        headers = dict(headers)

    response_dict = {
        'headers': headers,
        'status_code': http_response.status_code,
    }

    if response_dict['status_code'] >= 300:
        response_dict['body'] = await read(http_response)
    elif operation_model.has_streaming_output:
        response_dict['body'] = patch_stream(http_response.raw)
    else:
        response_dict['body'] = await read(http_response)
    return response_dict


async def read(http_response):
    body = await http_response.raw.read()
    http_response._content = body
    return body


def patch_stream(raw):
    raw.set_socket_timeout = noop
    return raw


def noop(*args, **kwargs):
    pass


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

    async def _send_request(self, request_dict, operation_model):
        attempts = 1
        request = self.create_request(request_dict, operation_model)
        success_response, exception = await self._get_response(
            request, operation_model, attempts)
        while await self._needs_retry(attempts, operation_model, request_dict,
                                      success_response, exception):
            attempts += 1
            # If there is a stream associated with the request, we need
            # to reset it before attempting to send the request again.
            # This will ensure that we resend the entire contents of the
            # body.
            request.reset_stream()
            # Create a new request when retried (including a new signature).
            request = self.create_request(
                request_dict, operation_model)
            success_response, exception = await self._get_response(
                request, operation_model, attempts)
        if exception is not None:
            raise exception
        else:
            return success_response

    async def _get_response(self, request, operation_model, attempts):
        # This will return a tuple of (success_response, exception)
        # and success_response is itself a tuple of
        # (http_response, parsed_dict).
        # If an exception occurs then the success_response is None.
        # If no exception occurs then exception is None.
        try:
            logger.debug("Sending http request: %s", request)
            headers = dict(self._headers(request.headers))
            http_response = await self.http_session.request(
                method=request.method, url=request.url, data=request.body,
                headers=headers, stream=True, verify=self.verify)
        except ConnectionError as e:
            # For a connection error, if it looks like it's a DNS
            # lookup issue, 99% of the time this is due to a misconfigured
            # region/endpoint so we'll raise a more specific error message
            # to help users.
            logger.debug("ConnectionError received when sending HTTP request.",
                         exc_info=True)
            if self._looks_like_dns_error(e):
                endpoint_url = e.request.url
                better_exception = EndpointConnectionError(
                    endpoint_url=endpoint_url, error=e)
                return (None, better_exception)
            elif self._looks_like_bad_status_line(e):
                better_exception = ConnectionClosedError(
                    endpoint_url=e.request.url, request=e.request)
                return (None, better_exception)
            else:
                return (None, e)
        except Exception as e:
            logger.debug("Exception received when sending HTTP request.",
                         exc_info=True)
            return (None, e)
        # This returns the http_response and the parsed_data.
        response_dict = await convert_to_response_dict(http_response,
                                                       operation_model)
        parser = self._response_parser_factory.create_parser(
            operation_model.metadata['protocol'])
        return ((http_response,
                 parser.parse(response_dict, operation_model.output_shape)),
                None)

    # CUT AND PASTE FROM BOTOCORE

    async def _needs_retry(self, attempts, operation_model, request_dict,
                           response=None, caught_exception=None):
        event_name = 'needs-retry.%s.%s' % (self._endpoint_prefix,
                                            operation_model.name)
        responses = self._event_emitter.emit(
            event_name, response=response, endpoint=self,
            operation=operation_model, attempts=attempts,
            caught_exception=caught_exception, request_dict=request_dict)
        handler_response = first_non_none_response(responses)
        if handler_response is None:
            return False
        else:
            # Request needs to be retried, and we need to sleep
            # for the specified number of times.
            logger.debug("Response received to retry, sleeping for "
                         "%s seconds", handler_response)

            # END OF CUT AND PASTE
            await asyncio.sleep(handler_response, loop=self._loop)
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

    def create_endpoint(self, service_model, region_name, endpoint_url,
                        verify=None, response_parser_factory=None,
                        timeout=DEFAULT_TIMEOUT,
                        max_pool_connections=MAX_POOL_CONNECTIONS):
        if not is_valid_endpoint_url(endpoint_url):
            raise ValueError("Invalid endpoint: %s" % endpoint_url)
        return AsyncEndpoint(
            self.http_session,
            endpoint_url,
            endpoint_prefix=service_model.endpoint_prefix,
            event_emitter=self._event_emitter,
            proxies=self._get_proxies(endpoint_url),
            verify=self._get_verify_value(verify),
            timeout=timeout,
            max_pool_connections=max_pool_connections,
            response_parser_factory=response_parser_factory)
