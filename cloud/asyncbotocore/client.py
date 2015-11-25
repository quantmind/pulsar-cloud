import copy
import asyncio

import botocore.client
import botocore.serialize
import botocore.parsers
from botocore.exceptions import ClientError
from botocore.signers import RequestSigner

from .endpoint import AsyncEndpointCreator


class AsyncClientCreator(botocore.client.ClientCreator):

    def __init__(self, loader, endpoint_resolver, user_agent, event_emitter,
                 retry_handler_factory, retry_config_translator,
                 response_parser_factory=None, loop=None, http_client=None):
        super().__init__(loader, endpoint_resolver, user_agent, event_emitter,
                         retry_handler_factory, retry_config_translator,
                         response_parser_factory=response_parser_factory)

        self._loop = loop or asyncio.get_event_loop()
        self.http_client = http_client

    def _get_client_args(self, service_model, region_name, is_secure,
                         endpoint_url, verify, credentials,
                         scoped_config, client_config):

        protocol = service_model.metadata['protocol']
        serializer = botocore.serialize.create_serializer(
            protocol, include_validation=True)

        event_emitter = copy.copy(self._event_emitter)

        endpoint_creator = AsyncEndpointCreator(self._endpoint_resolver,
                                                region_name, event_emitter,
                                                self._user_agent,
                                                loop=self._loop,
                                                http_client=self.http_client)

        endpoint = endpoint_creator.create_endpoint(
            service_model, region_name, is_secure=is_secure,
            endpoint_url=endpoint_url, verify=verify,
            response_parser_factory=self._response_parser_factory)

        response_parser = botocore.parsers.create_parser(protocol)

        if region_name is None:
            if client_config and client_config.region_name is not None:
                region_name = client_config.region_name

        signature_version, region_name =\
            self._get_signature_version_and_region(
                service_model, region_name, is_secure, scoped_config,
                endpoint_url)

        if client_config and client_config.signature_version is not None:
            signature_version = client_config.signature_version

        user_agent = self._user_agent

        if client_config is not None:
            if client_config.user_agent is not None:
                user_agent = client_config.user_agent
            if client_config.user_agent_extra is not None:
                user_agent += ' %s' % client_config.user_agent_extra

        signer = RequestSigner(service_model.service_name, region_name,
                               service_model.signing_name,
                               signature_version, credentials,
                               event_emitter)

        client_config = botocore.client.Config(
            region_name=region_name,
            signature_version=signature_version,
            user_agent=user_agent)

        return {
            'serializer': serializer,
            'endpoint': endpoint,
            'response_parser': response_parser,
            'event_emitter': event_emitter,
            'request_signer': signer,
            'service_model': service_model,
            'loader': self._loader,
            'client_config': client_config
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

    @asyncio.coroutine
    def _make_api_call(self, operation_name, api_params):
        operation_model = self._service_model.operation_model(operation_name)
        request_dict = self._convert_to_request_dict(
            api_params, operation_model)

        http, parsed_response = yield from self._endpoint.make_request(
            operation_model, request_dict)

        self.meta.events.emit(
            'after-call.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            http_response=http, parsed=parsed_response,
            model=operation_model
        )

        if http.status_code >= 300:
            raise ClientError(parsed_response, operation_name)
        return parsed_response

    def close(self):
        self._endpoint._connector.close()
