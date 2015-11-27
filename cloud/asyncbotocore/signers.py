import asyncio

from botocore.signers import RequestSigner
from botocore import UNSIGNED


class AsyncRequestSigner(RequestSigner):
    def __init__(self, service_name, region_name, signing_name,
                 signature_version, credentials, event_emitter):
        super().__init__(service_name, region_name, signing_name,
                         signature_version, credentials, event_emitter)
        self._service_name = service_name
        self._region_name = region_name
        self._signing_name = signing_name
        self._signature_version = signature_version
        self._credentials = credentials
        self._event_emitter = event_emitter

    @asyncio.coroutine
    def sign(self, operation_name, request):
        """
        Sign a request before it goes out over the wire.

        :type operation_name: string
        :param operation_name: The name of the current operation, e.g.
                               ``ListBuckets``.
        :type request: AWSRequest
        :param request: The request object to be sent over the wire.
        """
        signature_version = self._signature_version
        # Allow overriding signature version. A response of a blank
        # string means no signing is performed. A response of ``None``
        # means that the default signing method is used.
        handler, response = yield from self._event_emitter.emit_until_response(
            'choose-signer.{0}.{1}'.format(self._service_name, operation_name),
            signing_name=self._signing_name, region_name=self._region_name,
            signature_version=signature_version)
        if response is not None:
            signature_version = response

        # Allow mutating request before signing
        yield from self._event_emitter.emit(
            'before-sign.{0}.{1}'.format(self._service_name, operation_name),
            request=request, signing_name=self._signing_name,
            region_name=self._region_name,
            signature_version=signature_version, request_signer=self)

        # Sign the request if the signature version isn't None or blank
        if signature_version != UNSIGNED:
            signer = self.get_auth(self._signing_name, self._region_name,
                                   signature_version)
            signer.add_auth(request=request)
