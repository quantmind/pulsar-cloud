import asyncio

import botocore.session
import botocore.credentials
from botocore import utils, retryhandler, translate
from botocore.loaders import create_loader
from botocore.parsers import ResponseParserFactory
from botocore import credentials as botocredentials
import botocore.regions

from .client import AsyncClientCreator
from .hooks import AsyncHierarchicalEmitter


class AsyncSession(botocore.session.Session):
    def __init__(self, session_vars=None, event_hooks=None,
                 include_builtin_handlers=True, loader=None, loop=None):

        super().__init__(session_vars=session_vars, event_hooks=event_hooks,
                         include_builtin_handlers=include_builtin_handlers)
        self._loop = loop
        self._loader = loader
        if event_hooks is None:
            self._events = AsyncHierarchicalEmitter()
        else:
            self._events = event_hooks
        self._components = ComponentLocator()
        self._register_components()

    def _register_components(self):
        self._register_credential_provider()
        self._register_data_loader()
        self._register_endpoint_resolver()
        self._register_event_emitter()
        self._register_response_parser_factory()

    def _register_event_emitter(self):
        self._components.register_component('event_emitter', self._events)

    def _register_credential_provider(self):
        self._components.lazy_register_component(
            'credential_provider',
            lambda:  botocredentials.create_credential_resolver(self))

    def _register_data_loader(self):
        self._components.lazy_register_component(
            'data_loader',
            lambda:  create_loader(self.get_config_variable('data_path')))

    def _register_endpoint_resolver(self):
        self._components.lazy_register_component(
            'endpoint_resolver',
            lambda:  botocore.regions.EndpointResolver(
                self.get_data('_endpoints')))

    def _register_response_parser_factory(self):
        self._components.register_component('response_parser_factory',
                                            ResponseParserFactory())

    def get_component(self, name):
        return self._components.get_component(name)

    def register_component(self, name, component):
        self._components.register_component(name, component)

    def lazy_register_component(self, name, component):
        self._components.lazy_register_component(name, component)

    def create_client(self, service_name, region_name=None, api_version=None,
                      use_ssl=True, verify=None, endpoint_url=None,
                      aws_access_key_id=None, aws_secret_access_key=None,
                      aws_session_token=None, config=None, http_client=None):

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
            loader, endpoint_resolver, self.user_agent(), event_emitter,
            retryhandler, translate, response_parser_factory, loop=self._loop,
            http_client=http_client)
        client = client_creator.create_client(
            service_name, region_name, use_ssl, endpoint_url, verify,
            credentials, scoped_config=self.get_scoped_config(),
            client_config=config)
        return client


class ComponentLocator(object):
    """Service locator for session components."""
    def __init__(self):
        self._components = {}
        self._deferred = {}

    def get_component(self, name):
        if name in self._deferred:
            factory = self._deferred.pop(name)
            self._components[name] = factory()
        try:
            return self._components[name]
        except KeyError:
            raise ValueError("Unknown component: %s" % name)

    def register_component(self, name, component):
        self._components[name] = component
        try:
            del self._deferred[name]
        except KeyError:
            pass

    def lazy_register_component(self, name, no_arg_factory):
        self._deferred[name] = no_arg_factory
        try:
            del self._components[name]
        except KeyError:
            pass


def get_session(*, env_vars=None, loop=None):
    """
    Return a new session object.
    """
    loop = loop or asyncio.get_event_loop()
    return AsyncSession(session_vars=env_vars, loop=loop)
