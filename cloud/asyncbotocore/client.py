import botocore.parsers
from botocore.exceptions import ClientError, OperationNotPageableError
from botocore.utils import get_service_module_name
from botocore.paginate import Paginator

from .paginate import AsyncPageIterator
from .args import AsyncClientArgsCreator


class AsyncClientCreator(botocore.client.ClientCreator):

    def __init__(self, http_session, *args, **kw):
        super().__init__(*args, **kw)
        self.http_session = http_session

    @property
    def _loop(self):
        return self.http_session._loop

    def _get_client_args(self, service_model, region_name, is_secure,
                         endpoint_url, verify, credentials,
                         scoped_config, client_config, endpoint_bridge):
        args_creator = AsyncClientArgsCreator(
            self._event_emitter, self._user_agent,
            self._response_parser_factory, self._loader)
        return args_creator.get_client_args(
            service_model, region_name, is_secure, endpoint_url,
            verify, credentials, scoped_config, client_config, endpoint_bridge,
            self.http_session)

    def _create_client_class(self, service_name, service_model):
        class_attributes = self._create_methods(service_model)
        py_name_to_operation_name = self._create_name_mapping(service_model)
        class_attributes['_PY_TO_OP_NAME'] = py_name_to_operation_name
        bases = [AsyncBaseClient]
        self._event_emitter.emit('creating-client-class.%s' % service_name,
                                 class_attributes=class_attributes,
                                 base_classes=bases)
        class_name = get_service_module_name(service_model)
        cls = type(str(class_name), tuple(bases), class_attributes)
        return cls


class AsyncBaseClient(botocore.client.BaseClient):

    @property
    def http_session(self):
        return self._endpoint.http_session

    @property
    def _loop(self):
        return self._endpoint._loop

    async def _make_api_call(self, operation_name, api_params):
        operation_model = self._service_model.operation_model(operation_name)
        request_context = {
            'client_region': self.meta.region_name,
            'client_config': self.meta.config,
            'has_streaming_input': operation_model.has_streaming_input
        }
        request_dict = self._convert_to_request_dict(
            api_params, operation_model, context=request_context)

        self.meta.events.emit(
            'before-call.{endpoint_prefix}.{operation_name}'.format(
                endpoint_prefix=self._service_model.endpoint_prefix,
                operation_name=operation_name),
            model=operation_model, params=request_dict,
            request_signer=self._request_signer, context=request_context
        )

        http, parsed_response = await self._endpoint.make_request(
            operation_model, request_dict)

        self.meta.events.emit(
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

    def get_paginator(self, operation_name):
        """Create a paginator for an operation.
        :type operation_name: string
        :param operation_name: The operation name.  This is the same name
            as the method name on the client.  For example, if the
            method name is ``create_foo``, and you'd normally invoke the
            operation as ``client.create_foo(**kwargs)``, if the
            ``create_foo`` operation can be paginated, you can use the
            call ``client.get_paginator("create_foo")``.
        :raise OperationNotPageableError: Raised if the operation is not
            pageable.  You can use the ``client.can_paginate`` method to
            check if an operation is pageable.
        :rtype: L{botocore.paginate.Paginator}
        :return: A paginator object.
        """
        if not self.can_paginate(operation_name):
            raise OperationNotPageableError(operation_name=operation_name)
        else:
            actual_operation_name = self._PY_TO_OP_NAME[operation_name]
            # substitute iterator with async one
            Paginator.PAGE_ITERATOR_CLS = AsyncPageIterator
            paginator = Paginator(
                getattr(self, operation_name),
                self._cache['page_config'][actual_operation_name])
            return paginator

    async def __aenter__(self):
        await self.http_session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.http_session.__aexit__(exc_type, exc_val, exc_tb)

    def close(self):
        return self.http_session.close()
