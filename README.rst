Pulsar Cloud
================


Requirements
---------------

* Python 3.4 or above
* pulsar_
* botocore_
* greenlet_


Botocore
------------
Asynchronous implementation of botocore_ with pulsar_ and greenlet_.
Usage:

.. code:: python

    from cloud import Botocore

    ec2 = Botocore('ec2', 'us-east-1')
    ec2.describe_spot_price_history()


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

.. _pusher: https://pusher.com/
.. _pulsar: https://github.com/quantmind/pulsar
.. _botocore: https://github.com/boto/botocore
.. _greenlet: https://greenlet.readthedocs.org/en/latest/
