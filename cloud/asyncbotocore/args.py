import copy

import botocore.serialize
from botocore.args import ClientArgsCreator
from botocore.signers import RequestSigner

from .endpoint import AsyncEndpointCreator
from .config import AsyncConfig


class AsyncClientArgsCreator(ClientArgsCreator):

    def get_client_args(self, service_model, region_name, is_secure,
                        endpoint_url, verify, credentials, scoped_config,
                        client_config, endpoint_bridge, http_session):
        # CUT AND PASTE FROM BOTOCORE

        final_args = self.compute_client_args(
            service_model, client_config, endpoint_bridge, region_name,
            endpoint_url, is_secure, scoped_config)

        service_name = final_args['service_name']
        parameter_validation = final_args['parameter_validation']
        endpoint_config = final_args['endpoint_config']
        protocol = final_args['protocol']
        config_kwargs = final_args['config_kwargs']
        s3_config = final_args['s3_config']
        partition = endpoint_config['metadata'].get('partition', None)

        event_emitter = copy.copy(self._event_emitter)
        signer = RequestSigner(
            service_name, endpoint_config['signing_region'],
            endpoint_config['signing_name'],
            endpoint_config['signature_version'],
            credentials, event_emitter)

        # Add any additional s3 configuration for client
        config_kwargs['s3'] = s3_config
        self._conditionally_unregister_fix_s3_host(endpoint_url, event_emitter)

        # END OF CUT AND PASTE

        new_config = AsyncConfig(**config_kwargs)
        endpoint_creator = AsyncEndpointCreator(http_session, event_emitter)

        # CUT AND PASTE FROM BOTOCORE

        endpoint = endpoint_creator.create_endpoint(
            service_model, region_name=endpoint_config['region_name'],
            endpoint_url=endpoint_config['endpoint_url'], verify=verify,
            response_parser_factory=self._response_parser_factory,
            max_pool_connections=new_config.max_pool_connections,
            timeout=(new_config.connect_timeout, new_config.read_timeout))

        serializer = botocore.serialize.create_serializer(
            protocol, parameter_validation)
        response_parser = botocore.parsers.create_parser(protocol)
        return {
            'serializer': serializer,
            'endpoint': endpoint,
            'response_parser': response_parser,
            'event_emitter': event_emitter,
            'request_signer': signer,
            'service_model': service_model,
            'loader': self._loader,
            'client_config': new_config,
            'partition': partition
        }
