import os
import mimetypes
import asyncio

from cloud.asyncbotocore.session import get_session
from .sock import wrap_poolmanager, StreamingBodyWsgiIterator

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
            resp = yield from self._multipart(file, params)
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

    @asyncio.coroutine
    def _multipart(self, filename, params):
        response = yield from self.create_multipart_upload(**params)
        bucket = params['Bucket']
        key = params['Key']
        uid = response['UploadId']
        params['UploadId'] = uid
        params.pop('ContentType', None)
        try:
            parts = []
            with open(filename, 'rb') as file:
                while True:
                    body = file.read(MULTI_PART_SIZE)
                    if not body:
                        break
                    num = len(parts) + 1
                    params['Body'] = body
                    params['PartNumber'] = num
                    part = yield from self.upload_part(**params)
                    parts.append(dict(ETag=part['ETag'], PartNumber=num))
        except:
            yield from self.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=uid)
            raise
        else:
            if parts:
                all_parts = dict(Parts=parts)
                response = yield from self.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key,
                    MultipartUpload=all_parts)
                return response
            else:
                yield from self.abort_multipart_upload(Bucket=bucket, Key=key,
                                                       UploadId=uid)

    @asyncio.coroutine
    def _multipart_copy(self, source_bucket, source_key, bucket, key, size):
        response = yield from self.create_multipart_upload(Bucket=bucket,
                                                           Key=key)
        start = 0
        parts = []
        num = 1
        uid = response['UploadId']
        params = {
            'CopySource': self._source_string(source_bucket, source_key),
            'Bucket': bucket,
            'Key': key,
            'UploadId': uid
        }
        try:
            while start < size:
                end = min(size, start + MULTI_PART_SIZE)
                params['PartNumber'] = num
                params['CopySourceRange'] = 'bytes={}-{}'.format(start, end-1)
                part = yield from self.upload_part_copy(**params)
                parts.append(dict(
                    ETag=part['CopyPartResult']['ETag'], PartNumber=num))
                start = end
                num += 1
        except:
            yield from self.abort_multipart_upload(Bucket=bucket, Key=key,
                                                   UploadId=uid)
            raise
        else:
            if parts:
                all = dict(Parts=parts)
                response = yield from self.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=all)
                return response
            else:
                yield from self.abort_multipart_upload(Bucket=bucket, Key=key,
                                                       UploadId=uid)

    @asyncio.coroutine
    def copy_storage_object(self, source_bucket, source_key, bucket, key):
        info = yield from self.head_object(
            Bucket=source_bucket, Key=source_key)

        size = info['ContentLength']

        if size > MULTI_PART_SIZE:
            response = yield from self._multipart_copy(
                source_bucket, source_key, bucket, key, size)
            return response
        else:
            response = yield from self.copy_object(
                Bucket=bucket, Key=key,
                CopySource=self._source_string(source_bucket, source_key)
            )
            return response

    def _source_string(self, bucket, key):
        return '{}/{}'.format(bucket, key)

    def wsgi_stream_body(self, body, n=-1):
        '''WSGI iterator of a botocore StreamingBody
        '''
        return StreamingBodyWsgiIterator(body, self._async_call, n)
