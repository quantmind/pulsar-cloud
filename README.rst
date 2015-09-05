Pulsar Cloud
================

A client and server pusher_ implementation using
pulsar_ asynchronous framework.

Requirements
---------------

* Python 3.4 or above
* pulsar_
* botocore_


Usage
---------------

Create a pusher instance

.. code:: python

    from pusher import Pusher

    pusher = Pusher(app_id, key, secret)


Subscribe to a channel as a client

.. code:: python

    channel = yield from pusher.subscribe('test_channel')
    channel.bind('event', mycallback)

.. _pusher: https://pusher.com/
.. _pulsar: https://github.com/quantmind/pulsar
.. _botocore: https://github.com/quantmind/pulsar
