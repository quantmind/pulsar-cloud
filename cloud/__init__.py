import os

VERSION = (0, 3, 1, 'final', 0)

__version__ = '.'.join((str(v) for v in VERSION))
__author__ = "Luca Sbardella"
__contact__ = "luca@quantmind.com"
__homepage__ = "https://github.com/quantmind/pulsar-cloud"


if os.environ.get('pulsar_cloud_setup') != 'yes':
    from pulsar.utils.version import get_version

    from .pulsar_botocore import Botocore
    from .pusher import Pusher

    __version__ = get_version(VERSION)

    __all__ = ['Botocore', 'Pusher']
