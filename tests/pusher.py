import os
import unittest
import asyncio
from unittest import SkipTest


class PusherTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.pusher = cls.newPusher()

    @classmethod
    def newPusher(cls):
        from cloud import Pusher
        cfg = cls.cfg.params
        app_id = cfg.get('PUSHER_APP_ID', os.environ.get('PUSHER_APP_ID'))
        app_key = cfg.get('PUSHER_APP_KEY', os.environ.get('PUSHER_APP_KEY'))
        app_secret = cfg.get('PUSHER_APP_SECRET',
                             os.environ.get('PUSHER_APP_SECRET'))
        if not app_id:
            raise SkipTest('Requires pusher_app_id in tests/config.py')
        if not app_key:
            raise SkipTest('Requires pusher_app_key in tests/config.py')
        if not app_secret:
            raise SkipTest('Requires pusher_app_secret in tests/config.py')
        return Pusher(app_id, key=app_key, secret=app_secret)

    def test_pusher(self):
        pusher = self.pusher
        self.assertTrue(pusher.app_id)
        self.assertTrue(pusher.host)
        self.assertEqual(pusher.port, 80)

    def test_channel(self):
        pusher = self.pusher
        channel = pusher['test_channel']
        self.assertTrue(channel)
        self.assertEqual(channel.name, 'test_channel')
        self.assertEqual(channel.path,
                         '/apps/%s/channels/test_channel/events' %
                         pusher.app_id)
        self.assertEqual(channel.pusher, pusher)
        self.assertEqual(channel, pusher['test_channel'])

    def test_bind(self):
        pusher = self.pusher

        def test_data(data, event=None):
            try:
                self.assertEqual(event, 'hello')
                self.assertEqual(data['message'], 'Hello world')
            except Exception as exc:
                future.set_exception(exc)
            else:
                future.set_result(None)

        channel = yield from pusher.subscribe('test_channel2')
        channel.bind('hello', test_data)

        future = asyncio.Future()
        result = yield from channel.trigger('hello',
                                            {'message': 'Hello world'})
        self.assertEqual(result, True)
        yield from future
