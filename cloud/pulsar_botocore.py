from functools import partial

import botocore.session
import botocore.response
import botocore.parsers
from botocore.exceptions import ClientError

from .sock import wrap_poolmanager


class Botocore(object):
    '''An asynchronous wrapper for botocore
    '''
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, session=None, **kwargs):
        self.session = session or botocore.session.get_session()
        self.client = self.session.create_client(service_name,
                                                 region_name=region_name,
                                                 endpoint_url=endpoint_url,
                                                 **kwargs)
        # self.client._make_api_call = self._make_api_call
        # self.client._endpoint._send_request = self._send_request
        # self.client._endpoint._get_response = self._get_response
        endpoint = self.client._endpoint
        for adapter in endpoint.http_session.adapters.values():
            adapter.poolmanager = wrap_poolmanager(adapter.poolmanager)

    def __getattr__(self, operation):
        return partial(self.call, operation)

    def call(self, operation, **kwargs):
        return self.client._make_api_call(operation, kwargs)

    def _make_api_call(self, operation_name, api_params):
        client = self.client
        operation_model = client._service_model.operation_model(operation_name)
        request_dict = client._convert_to_request_dict(
            api_params, operation_model)
        http, parsed_response = yield from client._endpoint.make_request(
            operation_model, request_dict)

        client.meta.events.emit(
            'after-call.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            http_response=http, parsed=parsed_response,
            model=operation_model
        )

        if http.status_code >= 300:
            raise ClientError(parsed_response, operation_name)
        else:
            return parsed_response

    def _send_request(self, request_dict, operation_model):
        attempts = 1
        endpoint = self.client._endpoint
        request = endpoint.create_request(request_dict, operation_model)
        success_response, exception = yield from endpoint._get_response(
            request, operation_model, attempts)
        while endpoint._needs_retry(attempts, operation_model,
                                    success_response, exception):
            attempts += 1
            # If there is a stream associated with the request, we need
            # to reset it before attempting to send the request again.
            # This will ensure that we resend the entire contents of the
            # body.
            request.reset_stream()
            # Create a new request when retried (including a new signature).
            request = endpoint.create_request(
                request_dict, operation_model=operation_model)
            success_response, exception = yield from endpoint._get_response(
                request, operation_model, attempts)
        if exception is not None:
            raise exception
        else:
            return success_response

    def _get_response(self, request, operation_model, attempts):
        # This will return a tuple of (success_response, exception)
        # and success_response is itself a tuple of
        # (http_response, parsed_dict).
        # If an exception occurs then the success_response is None.
        # If no exception occurs then exception is None.
        endpoint = self.client._endpoint
        try:
            http_response = endpoint.http_session.send(
                request, verify=endpoint.verify,
                stream=operation_model.has_streaming_output,
                proxies=endpoint.proxies, timeout=endpoint.timeout)
        except ConnectionError as e:
            # For a connection error, if it looks like it's a DNS
            # lookup issue, 99% of the time this is due to a misconfigured
            # region/endpoint so we'll raise a more specific error message
            # to help users.
            if endpoint._looks_like_dns_error(e):
                endpoint_url = e.request.url
                better_exception = EndpointConnectionError(
                    endpoint_url=endpoint_url, error=e)
                return (None, better_exception)
            else:
                return (None, e)
        except Exception as e:
            logger.debug("Exception received when sending HTTP request.",
                         exc_info=True)
            return (None, e)
        # This returns the http_response and the parsed_data.
        response_dict = convert_to_response_dict(http_response,
                                                 operation_model)
        parser = endpoint._response_parser_factory.create_parser(
            operation_model.metadata['protocol'])
        return ((http_response, parser.parse(response_dict,
                                             operation_model.output_shape)),
                None)
