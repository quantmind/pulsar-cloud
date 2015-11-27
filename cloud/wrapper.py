import os
import mimetypes
import asyncio

from cloud.asyncbotocore.session import get_session
from .sock import wrap_poolmanager

# 8MB for multipart uploads
MULTI_PART_SIZE = 2**23


class AsyncBotocore(object):
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, loop=None, session=None, http_client=None,
                 **kwargs):
        loop = loop or None
        self.session = session or get_session(loop=loop)
        self.client = self.session.create_client(
            service_name, region_name=region_name, endpoint_url=endpoint_url,
            http_client=http_client, **kwargs)
        endpoint = self.client._endpoint
        for adapter in endpoint.http_session.adapters.values():
            adapter.poolmanager = wrap_poolmanager(adapter.poolmanager)

    def __getattr__(self, operation):
        return getattr(self.client, operation)

    @asyncio.coroutine
    def upload_file(self, bucket, file, uploadpath=None, key=None,
                    ContentType=None, **kw):
        if hasattr(file, 'read'):
            if hasattr(file, 'seek'):
                file.seek(0)
            file = file.read()

        is_file = False
        if not isinstance(file, str):
            size = len(file)
        else:
            is_file = True
            size = os.stat(file).st_size
            if not key:
                key = os.path.basename(file)
            if not ContentType:
                ContentType, _ = mimetypes.guess_type(file)

        assert key, 'key not available'

        if uploadpath:
            if not uploadpath.endswith('/'):
                uploadpath = '%s/' % uploadpath
            key = '%s%s' % (uploadpath, key)

        params = dict(Bucket=bucket, Key=key)
        if ContentType:
            params['ContentType'] = ContentType

        if size > MULTI_PART_SIZE and is_file:
            resp = self._multipart(file, params)
        elif is_file:
            with open(file, 'rb') as fp:
                params['Body'] = fp.read()
            resp = yield from self.put_object(**params)
        else:
            params['Body'] = file
            resp = yield from self.put_object(**params)
        if 'Key' not in resp:
            resp['Key'] = key
        if 'Bucket' not in resp:
            resp['Bucket'] = bucket
        return resp
