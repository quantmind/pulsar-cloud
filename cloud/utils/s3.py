import os
import mimetypes

# 8MB for multipart uploads
MULTI_PART_SIZE = 2**23


class S3tools:

    def upload_file(self, bucket, file, uploadpath=None, key=None,
                    ContentType=None, **kw):
        '''Upload a file to S3 possibly using the multi-part uploader
        Return the key uploaded
        '''
        is_filename = False

        if hasattr(file, 'read'):
            if hasattr(file, 'seek'):
                file.seek(0)
            file = file.read()
            size = len(file)
        elif key:
            size = len(file)
        else:
            is_filename = True
            size = os.stat(file).st_size
            key = os.path.basename(file)

        assert key, 'key not available'

        if not ContentType:
            ContentType, _ = mimetypes.guess_type(key)

        if uploadpath:
            if not uploadpath.endswith('/'):
                uploadpath = '%s/' % uploadpath
            key = '%s%s' % (uploadpath, key)

        params = dict(Bucket=bucket, Key=key)
        if ContentType:
            params['ContentType'] = ContentType

        if size > MULTI_PART_SIZE and is_filename:
            resp = yield from self._multipart(file, params)
        elif is_filename:
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

    def copy_storage_object(self, source_bucket, source_key, bucket, key):
        """Copy a file from one bucket into another
        """
        info = yield from self.head_object(Bucket=source_bucket,
                                           Key=source_key)
        size = info['ContentLength']

        if size > MULTI_PART_SIZE:
            result = yield from self._multipart_copy(source_bucket, source_key,
                                                     bucket, key, size)
        else:
            result = yield from self.copy_object(
                Bucket=bucket, Key=key,
                CopySource=self._source_string(source_bucket, source_key)
            )
        return result

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
        except Exception:
            yield from self.abort_multipart_upload(Bucket=bucket, Key=key,
                                                   UploadId=uid)
            raise
        else:
            if parts:
                bits = dict(Parts=parts)
                result = yield from self.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=bits)
                return result
            else:
                yield from self.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=uid)

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
                bits = dict(Parts=parts)
                result = yield from self.complete_multipart_upload(
                    Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=bits)
                return result
            else:
                yield from self.abort_multipart_upload(Bucket=bucket, Key=key,
                                                       UploadId=uid)

    def _source_string(self, bucket, key):
        return '{}/{}'.format(bucket, key)
