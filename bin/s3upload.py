#!/usr/bin/env python
import argparse
import logging
import asyncio


parser = argparse.ArgumentParser(
    description='Upload a file or a directory to s3')
parser.add_argument('path', nargs=1,
                    help='location of files to upload')
parser.add_argument('--bucket', '-b', nargs=1, required=True,
                    help='Bucket name to upload files to')
parser.add_argument('--region', '-r', dest='region', default='us-east-1',
                    help='S3 region to upload files')

args = parser.parse_args()


def upload(options, loop=None):
    from cloud.aws import AsyncioBotocore
    bucket = options.bucket[0]
    bits = bucket.split('/')
    bucket = bits[0]
    key = '/'.join(bits[1:])
    s3 = AsyncioBotocore('s3', options.region, loop=loop)
    return s3.upload_folder(bucket, options.path[0], key=key)


if __name__ == "__main__":
    logging.basicConfig(format='%(message)s', level=logging.INFO)
    loop = asyncio.get_event_loop()
    result = upload(parser.parse_args(), loop=loop)
    loop.run_until_complete(result)
