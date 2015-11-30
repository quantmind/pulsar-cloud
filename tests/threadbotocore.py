from .greenbotocore import unittest, BotocoreMixin


class BotocoreTestThread(BotocoreMixin, unittest.TestCase):

    def test_concurrency(self):
        self.assertEqual(self.s3.concurrency, 'thread')
