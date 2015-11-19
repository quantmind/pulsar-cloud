import botocore.session
import botocore.credentials
from botocore import utils

from pulsar import get_event_loop, new_event_loop


class PulsarSession(botocore.session.Session):
    def __init__(self, session_vars=None, event_hooks=None,
                 include_builtin_handlers=True, loader=None, loop=None):

        super().__init__(session_vars=session_vars, event_hooks=event_hooks,
                         include_builtin_handlers=include_builtin_handlers)

        self._loop = loop

    def create_client(self, service_name, region_name=None, api_version=None,
                      use_ssl=True, verify=None, endpoint_url=None,
                      aws_access_key_id=None, aws_secret_access_key=None,
                      aws_session_token=None, config=None):

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

        # TODO ClientCreator


def get_session(*, env_vars=None, loop=None):
    """
    Return a new session object.
    """
    loop = loop or get_event_loop() or new_event_loop()
    return PulsarSession(session_vars=env_vars, loop=loop)
