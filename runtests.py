import os
import sys
import asyncio
import unittest
from unittest import SkipTest

from pusher import Pusher, PusherChannel


class PusherTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.pusher = cls.newPusher()

    @classmethod
    def newPusher(cls):
        cfg = cls.cfg.params
        app_id = cfg.get('pusher_app_id')
        app_key = cfg.get('pusher_app_key')
        app_secret = cfg.get('pusher_app_secret')
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


def run():
    from pulsar.apps.test import TestSuite
    from pulsar.apps.test.plugins import bench, profile

    args = sys.argv
    if '--coveralls' in args:
        import quantflow
        from pulsar.utils.path import Path
        from pulsar.apps.test.cov import coveralls

        repo_token = None
        strip_dirs = [Path(quantflow.__file__).parent.parent, os.getcwd()]
        if os.path.isfile('.coveralls-repo-token'):
            with open('.coveralls-repo-token') as f:
                repo_token = f.read().strip()
        code = coveralls(strip_dirs=strip_dirs,
                         repo_token=repo_token)
        sys.exit(0)
    # Run the test suite
    #
    TestSuite(description='quantflow asynchronous test suite',
              modules=['runtests'],
              plugins=(bench.BenchMark(),
                       profile.Profile()),
              config='config.py').start()


if __name__ == '__main__':
    run()
