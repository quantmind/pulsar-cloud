Pulsar Cloud
================

:Master CI: |master-build|_ |coverage-master|
:Downloads: http://pypi.python.org/pypi/pulsar-cloud
:Source: https://github.com/quantmind/pulsar-cloud
:Mailing list: `google user group`_
:Design by: `Quantmind`_ and `Luca Sbardella`_
:Platforms: Linux, OSX, Windows. Python 3.4 and above
:Keywords: amazon, aws, botocore, pusher, websocket, async, pulsar, greenlet

.. |master-build| image:: https://travis-ci.org/quantmind/pulsar-cloud.svg?branch=master
.. _master-build: http://travis-ci.org/quantmind/pulsar-cloud
.. |coverage-master| image:: https://coveralls.io/repos/quantmind/pulsar-cloud/badge.svg?branch=master&service=github
  :target: https://coveralls.io/github/quantmind/pulsar-cloud?branch=master

Requirements
---------------

* Python 3.4 or above
* pulsar_
* botocore_
* greenlet_


Botocore
------------

This library provides two asynchornous implementations of botocore_.

**Asyncio Botocore**

The first implementation uses asyncio from the python standard libray only:

.. code:: python

    from cloud.aws import AsyncioBotocore

    s3 = AsyncioBotocore('s3', 'us-east-1')
    s3 = yield from s3.put_object(...)


**Green Botocore**

The second implementation, build on top of asyncio botocore, uses
pulsar_ and greenlet_ to obtain an implicit asynchronous behaviour.

Usage:

.. code:: python

    from cloud.aws import GreenBotocore
    from pulsar.apps.greenio import GreenPool

    def execute():
        s3 = GreenBotocore('s3', 'us-east-1')
        ec2.put_object(...)

    pool = GreenPool()
    yield from pool.submit(execute)


**S3 uploader script**

Usage:

    s3upload <path> bucket/my/location


Pusher
------------
A client and server pusher_ implementation using
pulsar_ asynchronous framework.
Create a pusher instance

.. code:: python

    from cloud import Pusher

    pusher = Pusher(app_id, key, secret)


Subscribe to a channel as a client

.. code:: python

    channel = yield from pusher.subscribe('test_channel')
    channel.bind('event', mycallback)

.. _`Luca Sbardella`: http://lucasbardella.com
.. _`Quantmind`: http://quantmind.com
.. _`google user group`: https://groups.google.com/forum/?fromgroups#!forum/python-pulsar
.. _pusher: https://pusher.com/
.. _pulsar: https://github.com/quantmind/pulsar
.. _botocore: https://github.com/boto/botocore
.. _greenlet: https://greenlet.readthedocs.org/en/latest/
