import copy

import botocore.client
import botocore.serialize
import botocore.parsers
from botocore.signers import RequestSigner
from pulsar import get_event_loop, new_event_loop

from .endpoint import PulsarEndpointCreator


class PulsarClientCreator(botocore.client.ClientCreator):

    def __init__(self, loader, endpoint_resolver, user_agent, event_emitter,
                 retry_handler_factory, retry_config_translator,
                 response_parser_factory=None, loop=None):
        super().__init__(loader, endpoint_resolver, user_agent, event_emitter,
                         retry_handler_factory, retry_config_translator,
                         response_parser_factory=response_parser_factory)

        loop = loop or get_event_loop() or new_event_loop()
        self._loop = loop

    def _get_client_args(self, service_model, region_name, is_secure,
                         endpoint_url, verify, credentials,
                         scoped_config, client_config):

        protocol = service_model.metadata['protocol']
        serializer = botocore.serialize.create_serializer(
            protocol, include_validation=True)

        event_emitter = copy.copy(self._event_emitter)

        endpoint_creator = PulsarEndpointCreator(self._endpoint_resolver,
                                                 region_name, event_emitter,
                                                 self._user_agent,
                                                 loop=self._loop)

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
