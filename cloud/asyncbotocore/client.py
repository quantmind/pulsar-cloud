import copy

import botocore.client
import botocore.serialize
import botocore.parsers
from botocore.exceptions import ClientError
from botocore.hooks import first_non_none_response
from botocore.awsrequest import prepare_request_dict

from .endpoint import AsyncEndpointCreator
from .signers import AsyncRequestSigner


class AsyncClientCreator(botocore.client.ClientCreator):

    def __init__(self, http_session, *args, **kw):
        super().__init__(*args, **kw)
        self.http_session = http_session

    def _get_client_args(self, service_model, region_name, is_secure,
                         endpoint_url, verify, credentials,
                         scoped_config, client_config):

        service_name = service_model.endpoint_prefix
        protocol = service_model.metadata['protocol']
        parameter_validation = True
        if client_config:
            parameter_validation = client_config.parameter_validation
        serializer = botocore.serialize.create_serializer(
            protocol, parameter_validation)

        event_emitter = copy.copy(self._event_emitter)
        response_parser = botocore.parsers.create_parser(protocol)
        endpoint_bridge = botocore.client.ClientEndpointBridge(
            self._endpoint_resolver, scoped_config, client_config,
            service_signing_name=service_model.metadata.get('signingName'))
        endpoint_config = endpoint_bridge.resolve(
            service_name, region_name, endpoint_url, is_secure)

        # Override the user agent if specified in the client config.
        user_agent = self._user_agent
        if client_config is not None:
            if client_config.user_agent is not None:
                user_agent = client_config.user_agent
            if client_config.user_agent_extra is not None:
                user_agent += ' %s' % client_config.user_agent_extra

        signer = AsyncRequestSigner(
            service_name, endpoint_config['signing_region'],
            endpoint_config['signing_name'],
            endpoint_config['signature_version'],
            credentials, event_emitter)

        # Create a new client config to be passed to the client based
        # on the final values. We do not want the user to be able
        # to try to modify an existing client with a client config.
        config_kwargs = dict(
            region_name=endpoint_config['region_name'],
            signature_version=endpoint_config['signature_version'],
            user_agent=user_agent)
        if client_config is not None:
            config_kwargs.update(
                connect_timeout=client_config.connect_timeout,
                read_timeout=client_config.read_timeout)

        # Add any additional s3 configuration for client
        self._inject_s3_configuration(
            config_kwargs, scoped_config, client_config)

        new_config = botocore.client.Config(**config_kwargs)
        endpoint_creator = AsyncEndpointCreator(self.http_session,
                                                event_emitter)
        endpoint = endpoint_creator.create_endpoint(
            service_model, region_name=endpoint_config['region_name'],
            endpoint_url=endpoint_config['endpoint_url'], verify=verify,
            response_parser_factory=self._response_parser_factory,
            timeout=(new_config.connect_timeout, new_config.read_timeout))

        return {
            'serializer': serializer,
            'endpoint': endpoint,
            'response_parser': response_parser,
            'event_emitter': event_emitter,
            'request_signer': signer,
            'service_model': service_model,
            'loader': self._loader,
            'client_config': new_config
        }

    def _create_client_class(self, service_name, service_model):
        class_attributes = self._create_methods(service_model)
        py_name_to_operation_name = self._create_name_mapping(service_model)
        class_attributes['_PY_TO_OP_NAME'] = py_name_to_operation_name
        bases = [AsyncBaseClient]
        self._event_emitter.emit('creating-client-class.%s' % service_name,
                                 class_attributes=class_attributes,
                                 base_classes=bases)
        cls = type(str(service_name), tuple(bases), class_attributes)
        return cls


class AsyncBaseClient(botocore.client.BaseClient):

    @property
    def _loop(self):
        return self._endpoint._loop

    async def _make_api_call(self, operation_name, api_params):
        request_context = {}
        operation_model = self._service_model.operation_model(operation_name)
        request_dict = await self._convert_to_request_dict(
            api_params, operation_model, context=request_context)

        handler, event_response = await self.meta.events.emit_until_response(
            'before-call.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            model=operation_model, params=request_dict,
            request_signer=self._request_signer, context=request_context)

        if event_response is not None:
            http, parsed_response = event_response
        else:
            http, parsed_response = await self._endpoint.make_request(
                operation_model, request_dict)

        await self.meta.events.async_emit(
            'after-call.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            http_response=http, parsed=parsed_response,
            model=operation_model, context=request_context
        )

        if http.status_code >= 300:
            raise ClientError(parsed_response, operation_name)
        else:
            return parsed_response

    async def _convert_to_request_dict(self, api_params, operation_model,
                                       context=None):
        # Given the API params provided by the user and the operation_model
        # we can serialize the request to a request_dict.
        operation_name = operation_model.name

        # Emit an event that allows users to modify the parameters at the
        # beginning of the method. It allows handlers to modify existing
        # parameters or return a new set of parameters to use.
        responses = await self.meta.events.async_emit(
            'provide-client-params.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            params=api_params, model=operation_model, context=context)
        api_params = first_non_none_response(responses, default=api_params)

        event_name = (
            'before-parameter-build.{endpoint_prefix}.{operation_name}')
        await self.meta.events.async_emit(
            event_name.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            params=api_params, model=operation_model, context=context)

        request_dict = self._serializer.serialize_to_request(
            api_params, operation_model)
        prepare_request_dict(request_dict, endpoint_url=self._endpoint.host,
                             user_agent=self._client_config.user_agent)
        return request_dict

    def close(self):
        self._endpoint._connector.close()
