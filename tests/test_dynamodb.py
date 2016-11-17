import time
import unittest

from cloud.aws import AsyncioBotocore

from tests import BotocoreMixin


class DynamoTest(BotocoreMixin, unittest.TestCase):

    @classmethod
    async def setUpClass(cls):
        super().setUpClass()
        cls.client = AsyncioBotocore('dynamodb', **cls.kwargs)
        cls.table_name = 'pulsarcloud_%d' % int(time.time())
        await cls.create_table()

    @classmethod
    async def tearDownClass(cls):
        await cls.delete_table()
        await cls.sessions.close()

    @classmethod
    async def is_table_ready(cls, table_name):
        response = await cls.client.describe_table(
            TableName=table_name
        )
        return response['Table']['TableStatus'] == 'ACTIVE'

    @classmethod
    async def create_table(cls, table_name=None):
        table_name = table_name or cls.table_name
        table_kwargs = {
            'TableName': table_name,
            'AttributeDefinitions': [
                {
                    'AttributeName': 'testKey',
                    'AttributeType': 'S'
                }
            ],
            'KeySchema': [
                {
                    'AttributeName': 'testKey',
                    'KeyType': 'HASH'
                }
            ],
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        }
        response = await cls.client.create_table(**table_kwargs)
        while not await cls.is_table_ready(table_name):
            pass
        cls.assert_status(response)
        return table_name

    @classmethod
    async def delete_table(cls, table_name=None):
        await cls.client.delete_table(table_name or cls.table_name)

    @classmethod
    async def put_item(cls, key_string_value, **item):
        item['testKey'] = {
            'S': key_string_value
        }
        response = await cls.client.put_item(
            TableName=cls.table_name,
            Item=item
        )
        cls.assert_status(response)

    async def test_get_item(self):
        test_key = 'testValue'
        await self.put_item(test_key, foo={'S': 't' * 2**13})
        response = await self.client.get_item(
            TableName=self.table_name,
            Key={
                'testKey': {
                    'S': test_key
                }
            },
        )
        self.assert_status(response)
        self.assertEqual(response['Item']['testKey'], {'S': test_key})
