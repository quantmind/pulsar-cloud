"""Utilities for S3 storage
"""
import os
import mimetypes
import logging
import asyncio

from pulsar.utils.system import convert_bytes

# 8MB for multipart uploads
MULTI_PART_SIZE = 2**23
LOGGER = logging.getLogger('cloud.s3')


def skip_file(filename):
    if filename.startswith('.') or filename.startswith('_'):
        return True


def s3_key(key):
    return key.replace('\\', '/')


class S3tools:
    """Mixin with additional s3 methods
    """
    async def upload_file(self, bucket, file, uploadpath=None, key=None,
                          ContentType=None, **kw):
        """Upload a file to S3 possibly using the multi-part uploader
        Return the key uploaded
        """
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

        if not ContentType:
            ContentType = 'application/octet-stream'

        params['ContentType'] = ContentType

        if size > MULTI_PART_SIZE and is_filename:
            resp = await _multipart(self, file, params)
        elif is_filename:
            with open(file, 'rb') as fp:
                params['Body'] = fp.read()
            resp = await self.put_object(**params)
        else:
            params['Body'] = file
            resp = await self.put_object(**params)
        if 'Key' not in resp:
            resp['Key'] = key
        if 'Bucket' not in resp:
            resp['Bucket'] = bucket
        return resp

    async def copy_storage_object(self, source_bucket, source_key,
                                  bucket, key):
        """Copy a file from one bucket into another
        """
        info = await self.head_object(Bucket=source_bucket, Key=source_key)
        size = info['ContentLength']

        if size > MULTI_PART_SIZE:
            result = await _multipart_copy(self, source_bucket, source_key,
                                           bucket, key, size)
        else:
            result = await self.copy_object(
                Bucket=bucket, Key=key,
                CopySource=_source_string(source_bucket, source_key)
            )
        return result

    def upload_folder(self, bucket, folder, key=None, skip=None,
                      content_types=None):
        """Recursively upload a ``folder`` into a backet.

        :param bucket: bucket where to upload the folder to
        :param folder: the folder location in the local file system
        :param key: Optional key where the folder is uploaded
        :param skip: Optional list of files to skip
        :param content_types: Optional dictionary mapping suffixes to
            content types
        :return: a coroutine
        """
        uploader = FolderUploader(self, bucket, folder, key, skip,
                                  content_types)
        return uploader.start()


# INTERNALS
async def _multipart(self, filename, params):
    response = await self.create_multipart_upload(**params)
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
                result = await self.upload_part(**params)
                part = result['ResponseMetadata']['HTTPHeaders']
                parts.append(dict(ETag=part['Etag'], PartNumber=num))
    except Exception:
        await self.abort_multipart_upload(Bucket=bucket, Key=key,
                                          UploadId=uid)
        raise
    else:
        if parts:
            bits = dict(Parts=parts)
            result = await self.complete_multipart_upload(
                Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=bits)
            return result
        else:
            await self.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=uid)


async def _multipart_copy(self, source_bucket, source_key, bucket,
                          key, size):
    response = await self.create_multipart_upload(Bucket=bucket, Key=key)
    start = 0
    parts = []
    num = 1
    uid = response['UploadId']
    params = {
        'CopySource': _source_string(source_bucket, source_key),
        'Bucket': bucket,
        'Key': key,
        'UploadId': uid
    }
    try:
        while start < size:
            end = min(size, start + MULTI_PART_SIZE)
            params['PartNumber'] = num
            params['CopySourceRange'] = 'bytes={}-{}'.format(start, end-1)
            part = await self.upload_part_copy(**params)
            parts.append(dict(
                ETag=part['CopyPartResult']['ETag'], PartNumber=num))
            start = end
            num += 1
    except:
        await self.abort_multipart_upload(Bucket=bucket, Key=key,
                                          UploadId=uid)
        raise
    else:
        if parts:
            bits = dict(Parts=parts)
            result = await self.complete_multipart_upload(
                Bucket=bucket, UploadId=uid, Key=key, MultipartUpload=bits)
            return result
        else:
            await self.abort_multipart_upload(Bucket=bucket, Key=key,
                                              UploadId=uid)


def _source_string(bucket, key):
    return '{}/{}'.format(bucket, key)


class FolderUploader:
    """Utility class to recursively upload a folder to S3
    """
    def __init__(self, botocore, bucket, folder, key=None, skip=None,
                 content_types=None):
        self.botocore = botocore
        self.bucket = bucket
        self.folder = folder
        self.all = {}
        self.failures = {}
        self.success = {}
        self.total_size = 0
        self.total_files = 0
        self.skip = set(skip or ())
        self.content_types = content_types or {}
        if not os.path.isdir(folder):
            raise ValueError('%s not a folder' % folder)
        if not key:
            base, key = os.path.split(folder)
            if not key:
                key = os.path.basename(base)
        if not key:
            raise ValueError('Could not calculate key from "%s"' % folder)
        self.key = key

    @property
    def _loop(self):
        return self.botocore._loop

    async def start(self):
        # Loop through all files and upload
        futures = []
        for dirpath, _, filenames in os.walk(self.folder):
            for filename in filenames:
                if skip_file(filename) or filename in self.skip:
                    continue
                full_path = os.path.join(dirpath, filename)
                futures.append(self._upload_file(full_path))
                self.all[full_path] = os.stat(full_path).st_size
        self.total_files = len(self.all)
        await asyncio.gather(*futures, loop=self._loop)
        failures = len(self.failures)
        total_files = self.total_files - failures
        LOGGER.info('Uploaded %d files for a total of %s. %d failures',
                    total_files, convert_bytes(self.total_size), failures)
        return dict(failures=self.failures,
                    files=self.success,
                    total_size=self.total_size)

    async def _upload_file(self, full_path):
        """Coroutine for uploading a single file
        """
        rel_path = os.path.relpath(full_path, self.folder)
        key = s3_key(os.path.join(self.key, rel_path))
        ct = self.content_types.get(key.split('.')[-1])
        with open(full_path, 'rb') as fp:
            file = fp.read()
        try:
            await self.botocore.upload_file(self.bucket, file, key=key,
                                            ContentType=ct)
        except Exception as exc:
            LOGGER.error('Could not upload "%s": %s', key, exc)
            self.failures[key] = self.all.pop(full_path)
            return
        size = self.all.pop(full_path)
        self.success[key] = size
        self.total_size += size
        percentage = 100*(1 - len(self.all)/self.total_files)
        message = '{0:.0f}% completed - uploaded "{1}" - {2}'.format(
            percentage, key, convert_bytes(size))
        LOGGER.info(message)
