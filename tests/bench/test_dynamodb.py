import asyncio
import unittest

from tests.test_dynamodb import DynamoMixin


class BenchmarkDynamoDb(DynamoMixin, unittest.TestCase):
    __benchmark__ = True
    __number__ = 1

    @classmethod
    async def setUpClass(cls):
        await super().setUpClass()
        await cls.put_item('bench1', foo={'S': 'dbsajcdsacs'})
        cls.kwargs = dict(
            TableName=cls.table_name,
            Key={
                'testKey': {
                    'S': 'bench1'
                }
            },
        )

    async def test_get_item(self):
        await asyncio.gather(*[self.client.get_item(**self.kwargs)
                               for _ in range(50)])
