import unittest

from pulsar import as_coroutine
from pulsar_botocore.session import get_session


class BotoTest(unittest.TestCase):

    def test_pulsar_botocore_can_make_request(self):
        session = get_session()
        client = session.create_client('ec2', 'us-east-1')
        result = yield from as_coroutine(client.describe_instances())
        self.assertEqual(result['ResponseMetadata']['HTTPStatusCode'], 200)
