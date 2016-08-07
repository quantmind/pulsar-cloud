import botocore.session
import botocore.credentials
from botocore.exceptions import PartialCredentialsError
from botocore import retryhandler, translate
import botocore.regions

from .client import AsyncClientCreator


class AsyncSession(botocore.session.Session):

    def create_client(self, service_name, region_name=None, api_version=None,
                      use_ssl=True, verify=None, endpoint_url=None,
                      aws_access_key_id=None, aws_secret_access_key=None,
                      aws_session_token=None, config=None, http_session=None):

        assert http_session, "http_session is required"

        # CUT AND PASTE FROM BOTOCORE

        default_client_config = self.get_default_client_config()
        # If a config is provided and a default config is set, then
        # use the config resulting from merging the two.
        if config is not None and default_client_config is not None:
            config = default_client_config.merge(config)
        # If a config was not provided then use the default
        # client config from the session
        elif default_client_config is not None:
            config = default_client_config

        # Figure out the user-provided region based on the various
        # configuration options.
        if region_name is None:
            if config and config.region_name is not None:
                region_name = config.region_name
            else:
                region_name = self.get_config_variable('region')

        # Figure out the verify value base on the various
        # configuration options.
        if verify is None:
            verify = self.get_config_variable('ca_bundle')

        if api_version is None:
            api_version = self.get_config_variable('api_versions').get(
                service_name, None)

        loader = self.get_component('data_loader')
        event_emitter = self.get_component('event_emitter')
        response_parser_factory = self.get_component(
            'response_parser_factory')
        if aws_access_key_id is not None and aws_secret_access_key is not None:
            credentials = botocore.credentials.Credentials(
                access_key=aws_access_key_id,
                secret_key=aws_secret_access_key,
                token=aws_session_token)
        elif self._missing_cred_vars(aws_access_key_id,
                                     aws_secret_access_key):
            raise PartialCredentialsError(
                provider='explicit',
                cred_var=self._missing_cred_vars(aws_access_key_id,
                                                 aws_secret_access_key))
        else:
            credentials = self.get_credentials()
        endpoint_resolver = self.get_component('endpoint_resolver')

        # END OF CUT AND PASTE

        client_creator = AsyncClientCreator(
            http_session,
            loader, endpoint_resolver, self.user_agent(), event_emitter,
            retryhandler, translate, response_parser_factory)

        client = client_creator.create_client(
            service_name=service_name, region_name=region_name,
            is_secure=use_ssl, endpoint_url=endpoint_url, verify=verify,
            credentials=credentials, scoped_config=self.get_scoped_config(),
            client_config=config, api_version=api_version)
        return client


def get_session(env_vars=None):
    """
    Return a new session object.
    """
    return AsyncSession(session_vars=env_vars)
