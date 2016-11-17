import os
import unittest
import string
import json
import asyncio

from pulsar.apps.http import HttpClient
from pulsar.utils.string import random_string

from cloud.aws import GreenBotocore
from cloud.utils.s3 import MULTI_PART_SIZE

from tests import RandomFile, BUCKET, BotocoreMixin, green


class AsyncioBotocoreTest(BotocoreMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.ec2 = GreenBotocore('ec2', **cls.kwargs)
        cls.s3 = GreenBotocore('s3', **cls.kwargs)

    def assert_s3_equal(self, body, copy_name):
        response = self.s3.get_object(Bucket=BUCKET, Key=copy_name)
        value = b''.join(response['Body'])
        if isinstance(body, str):
            with open(body, 'rb') as fo:
                body = fo.read()
        self.assertEqual(body, value)

    def _clean_up(self, key, size):
        response = self.s3.head_object(Bucket=BUCKET, Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentLength'], size)
        # Delete
        response = self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 204)
        # self.assertRaises(ClientError, self.s3.get_object,
        #                   Bucket=BUCKET, Key=key)

    def _create_object(self, key, body):
        response = self.s3.put_object(Bucket=BUCKET, Body=body,
                                      ContentType='text/plain',
                                      Key=key)
        self.assert_status(response)
        return response

    async def _asyncio_create_object(self, key, body):
        s3 = self.s3.client
        response = await s3.put_object(Bucket=BUCKET, Body=body,
                                       ContentType='text/plain',
                                       Key=key)
        self.assert_status(response)
        return response

    def _green_sleep(self, sleep):
        self.green_pool.wait(asyncio.sleep(sleep))

    def _test_copy(self, size):
        # Must be run with green decorated function
        with RandomFile(int(size)) as r:
            response = self.s3.upload_file(BUCKET, r.filename)
            self.assert_status(response)
            copy_key = 'copy_{}'.format(r.key)
            response = self.s3.copy_storage_object(
                BUCKET, r.key, BUCKET, copy_key)
            self.assert_status(response)
            self.assert_s3_equal(r.filename, copy_key)
            self._clean_up(r.key, r.size)
            self._clean_up(copy_key, r.size)

    def _fetch_all(self, pages):
        responses = []
        while True:
            n = pages.next_page()
            if n is None:
                break
            responses.append(n)
        return responses

    def test_no_http_session(self):
        cli = GreenBotocore('s3')
        self.assertTrue(cli._client)
        self.assertIsInstance(cli.http_session, HttpClient)
        self.assertEqual(cli.http_session._loop, asyncio.get_event_loop())

    def test_green_callable(self):
        call = self.ec2.describe_instances
        self.assertEqual(str(call), 'describe_instances')

    @green
    def test_describe_instances(self):
        response = self.ec2.describe_instances()
        self.assert_status(response)

    @green
    def test_describe_spot_price_history(self):
        response = self.ec2.describe_spot_price_history()
        self.assert_status(response)
        self.assertIsInstance(response['SpotPriceHistory'], list)

    @green
    def test_upload_text(self):
        with open(__file__, 'r') as f:
            body = f.read()
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = self.s3.put_object(Bucket=BUCKET, Body=body,
                                          ContentType='text/plain',
                                          Key=key)
            self.assert_status(response)

        # Read object
        response = self.s3.get_object(Bucket=BUCKET, Key=key)
        self.assert_status(response)
        self.assertEqual(response['ContentType'], 'text/plain')

        # Delete Object
        response = self.s3.delete_object(Bucket=BUCKET, Key=key)
        self.assert_status(response, 204)

    @green
    def test_upload_binary(self):
        with RandomFile(2**12) as r:
            response = self.s3.upload_file(BUCKET, r.filename)
            self.assert_status(response)
            self._clean_up(r.key, r.size)

    @green
    def test_upload_binary_large(self):
        with RandomFile(int(1.5*MULTI_PART_SIZE)) as r:
            response = self.s3.upload_file(BUCKET, r.filename)
            self.assert_status(response)
            self._clean_up(r.key, r.size)

    @green
    def test_copy(self):
        self._test_copy(2**12)

    @green
    def test_copy_large(self):
        self._test_copy(1.5*MULTI_PART_SIZE)

    @green
    def test_copy_json(self):
        data = {'test': 12345}
        text = json.dumps(data)
        size = len(text)
        key = '%s.json' % random_string(characters=string.ascii_letters)
        response = self.s3.upload_file(BUCKET, text, key=key)
        self.assert_status(response)
        copy_key = 'copy_{}'.format(key)
        response = self.s3.copy_storage_object(
            BUCKET, key, BUCKET, copy_key)
        self.assert_status(response)
        self.assert_s3_equal(text.encode('utf-8'), copy_key)
        self._clean_up(key, size)
        self._clean_up(copy_key, size)

    @green
    def test_copy_text(self):
        filename = __file__
        with open(filename, 'r') as f:
            size = len(f.read())

        with open(filename, 'r') as f:
            key = '%s.py' % random_string(characters=string.ascii_letters)
            response = self.s3.upload_file(BUCKET, f, key=key,
                                           ContentType='text/plain')
            self.assert_status(response)
            copy_key = 'copy_{}'.format(key)
            response = self.s3.copy_storage_object(
                BUCKET, key, BUCKET, copy_key)
            self.assert_status(response)
            self.assert_s3_equal(filename, copy_key)
            self._clean_up(key, size)
            self._clean_up(copy_key, size)

    @green
    def test_upload_folder(self):
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                            'docs', 'history')
        result = self.s3.upload_folder(BUCKET, path)
        self.assertTrue(result)
        self.assertFalse(result['failures'])
        self.assertTrue(result['files'])
        self.assertTrue(result['total_size'])

    @green
    def test_paginate(self):
        name = random_string()
        for i in range(5):
            key_name = '%s/key%d' % (name, i)
            self._create_object(key_name, 'bla')

        # Eventual consistency.
        self._green_sleep(3)
        paginator = self.s3.get_paginator('list_objects')
        pages = paginator.paginate(MaxKeys=1, Bucket=BUCKET, Prefix=name)
        responses = list(pages)
        # responses = self._fetch_all(pages)

        self.assertEqual(len(responses), 5)
        for i, el in enumerate(responses):
            key = el['Contents'][0]['Key']
            self.assertEqual(key, '%s/key%d' % (name, i))

    async def test_paginate_asyncio(self):
        name = random_string()
        for i in range(5):
            key_name = '%s/key%d' % (name, i)
            await self._asyncio_create_object(key_name, 'bla')

        await asyncio.sleep(3)
        s3 = self.s3.client
        paginator = s3.get_paginator('list_objects')
        pages = paginator.paginate(MaxKeys=1, Bucket=BUCKET, Prefix=name)
        responses = []
        async for page in pages:
            responses.append(page)

        self.assertEqual(len(responses), 5)
        for i, el in enumerate(responses):
            key = el['Contents'][0]['Key']
            self.assertEqual(key, '%s/key%d' % (name, i))

    async def test_paginate_asyncio_build_full_result(self):
        name = random_string()
        for i in range(5):
            key_name = '%s/key%d' % (name, i)
            await self._asyncio_create_object(key_name, 'bla')

        await asyncio.sleep(3)
        s3 = self.s3.client
        paginator = s3.get_paginator('list_objects')
        pages = paginator.paginate(MaxKeys=1, Bucket=BUCKET, Prefix=name)
        result = await pages.build_full_result()

        self.assertEqual(len(result), 1)
        responses = result['Contents']
        self.assertEqual(len(responses), 5)
        for i, el in enumerate(responses):
            key = el['Key']
            self.assertEqual(key, '%s/key%d' % (name, i))
