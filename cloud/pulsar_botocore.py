import os
import mimetypes

import botocore.session

from pulsar.apps.greenio import GreenPool, getcurrent

from .sock import wrap_poolmanager, StreamingBodyWsgiIterator

# 8MB for multipart uploads
MULTI_PART_SIZE = 2**23


class Botocore(object):
    '''An asynchronous wrapper for botocore
    '''
    def __init__(self, service_name, region_name=None,
                 endpoint_url=None, session=None, green_pool=None,
                 green=True, **kwargs):
        self._green_pool = green_pool
        self.session = session or botocore.session.get_session()
        self.client = self.session.create_client(service_name,
                                                 region_name=region_name,
                                                 endpoint_url=endpoint_url,
                                                 **kwargs)
        if green or green_pool:
            self._blocking_api_call = self.client._make_api_call
            self.client._make_api_call = self._make_api_call

            if green:
                endpoint = self.client._endpoint
                for adapter in endpoint.http_session.adapters.values():
                    adapter.poolmanager = wrap_poolmanager(adapter.poolmanager)

                self._blocking_call = self._green_call

            else:
                self._blocking_call = self._thread_call

        else:
            self._blocking_call = self._sync_call

    def __getattr__(self, operation):
        return getattr(self.client, operation)

    @property
    def concurrency(self):
        if self._blocking_call == self._thread_call:
            return 'thread'
        elif self._blocking_call == self._green_call:
            return 'green'

    def green_pool(self):
        if not self._green_pool:
            self._green_pool = GreenPool()
        return self._green_pool

    def wsgi_stream_body(self, body, n=-1):
        '''WSGI iterator of a botocore StreamingBody
        '''
        return StreamingBodyWsgiIterator(body, self._blocking_call, n)

    def upload_file(self, bucket, file, uploadpath=None, key=None,
                    ContentType=None, **kw):
        '''Upload a file to S3 possibly using the multi-part uploader

        Return the key uploaded
        '''
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
            resp = self.put_object(**params)
        else:
            params['Body'] = file
            resp = self.put_object(**params)
        if 'Key' not in resp:
            resp['Key'] = key
        if 'Bucket' not in resp:
            resp['Bucket'] = bucket
        return resp

    # INTERNALS

    def _make_api_call(self, operation, kwargs):
        return self._blocking_call(self._blocking_api_call, operation, kwargs)

    def _green_call(self, func, *args):
        if getcurrent().parent:
            return func(*args)
        else:
            pool = self.green_pool()
            return pool.submit(func, *args)

    def _thread_call(self, func, *args):
        '''A call using the event loop executor.
        '''
        pool = self.green_pool()
        loop = pool._loop
        return pool.wait(loop.run_in_executor(None, func, *args))

    def _sync_call(self, func, *args):
        return func(*args)

    def _multipart(self, filename, params):
        response = self.create_multipart_upload(**params)
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
                    part = self.upload_part(**params)
                    parts.append(dict(ETag=part['ETag'], PartNumber=num))
        except:
            self.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=uid)
            raise
        else:
            if parts:
                all = dict(Parts=parts)
                return self.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=all)
            else:
                self.abort_multipart_upload(Bucket=bucket, Key=key,
                                            UploadId=uid)

    def copy_storage_object(self, source_bucket, source_key, bucket, key):
        info = self.head_object(Bucket=source_bucket, Key=source_key)
        size = info['ContentLength']

        if size > MULTI_PART_SIZE:
            return self._multipart_copy(source_bucket, source_key,
                                        bucket, key, size)
        else:
            return self.copy_object(
                Bucket=bucket, Key=key,
                CopySource=self._source_string(source_bucket, source_key)
            )

    def _source_string(self, bucket, key):
        return '{}/{}'.format(bucket, key)

    def _multipart_copy(self, source_bucket, source_key, bucket, key, size):
        response = self.create_multipart_upload(Bucket=bucket, Key=key)
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
                part = self.upload_part_copy(**params)
                parts.append(dict(
                    ETag=part['CopyPartResult']['ETag'], PartNumber=num))
                start = end
                num += 1
        except:
            self.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=uid)
            raise
        else:
            if parts:
                all = dict(Parts=parts)
                return self.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=all)
            else:
                self.abort_multipart_upload(Bucket=bucket, Key=key,
                                            UploadId=uid)
