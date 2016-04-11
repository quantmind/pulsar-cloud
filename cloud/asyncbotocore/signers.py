from botocore.signers import RequestSigner
import botocore


class AsyncRequestSigner(RequestSigner):

    async def sign(self, operation_name, request):
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
        handler, response = await self._event_emitter.emit_until_response(
            'choose-signer.{0}.{1}'.format(self._service_name, operation_name),
            signing_name=self._signing_name, region_name=self._region_name,
            signature_version=signature_version)
        if response is not None:
            signature_version = response

        # Allow mutating request before signing
        await self._event_emitter.async_emit(
            'before-sign.{0}.{1}'.format(self._service_name, operation_name),
            request=request, signing_name=self._signing_name,
            region_name=self._region_name,
            signature_version=signature_version, request_signer=self)

        if signature_version != botocore.UNSIGNED:
            signer = self.get_auth_instance(self._signing_name,
                                            self._region_name,
                                            signature_version)
            signer.add_auth(request=request)
