pulsar-pusher
================

A client and server pusher_ implementation using
pulsar_ asynchronous framework.

Requirements
---------------

* Python 3.4 or above
* pulsar_ 3.4 or above


Usage
---------------

Create a pusher instance

.. code:: python

    from pusher import Pusher

    pusher = Pusher(app_id, key, secret)
    
    
.. _pusher: https://pusher.com/
.. _pulsar: https://github.com/quantmind/pulsar
