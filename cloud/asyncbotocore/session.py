import botocore.session
import botocore.credentials
from botocore import utils, retryhandler, translate
import botocore.regions

from .client import AsyncClientCreator
from .hooks import AsyncHierarchicalEmitter


class AsyncSession(botocore.session.Session):

    def __init__(self, event_hooks=None, **kw):
        if event_hooks is None:
            event_hooks = AsyncHierarchicalEmitter()
        super().__init__(event_hooks=event_hooks, **kw)

    def create_client(self, service_name, region_name=None, api_version=None,
                      use_ssl=True, verify=None, endpoint_url=None,
                      aws_access_key_id=None, aws_secret_access_key=None,
                      aws_session_token=None, config=None, http_session=None):

        assert http_session, "http_session is required"

        if endpoint_url is not None:
            self.unregister('before-sign.s3', utils.fix_s3_host)

        if region_name is None:
            region_name = self.get_config_variable('region')

        loader = self.get_component('data_loader')
        event_emitter = self.get_component('event_emitter')
        response_parser_factory = self.get_component(
            'response_parser_factory')
        if aws_secret_access_key is not None:
            credentials = botocore.credentials.Credentials(
                access_key=aws_access_key_id,
                secret_key=aws_secret_access_key,
                token=aws_session_token)
        else:
            credentials = self.get_credentials()
        endpoint_resolver = self.get_component('endpoint_resolver')

        client_creator = AsyncClientCreator(
            http_session,
            loader, endpoint_resolver, self.user_agent(), event_emitter,
            retryhandler, translate, response_parser_factory)

        return client_creator.create_client(
            service_name, region_name, use_ssl, endpoint_url, verify,
            credentials, scoped_config=self.get_scoped_config(),
            client_config=config)


def get_session(env_vars=None):
    """
    Return a new session object.
    """
    return AsyncSession(session_vars=env_vars)
