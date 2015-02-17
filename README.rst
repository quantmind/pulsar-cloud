pulsar-pusher
================

A client and server [pusher](https://pusher.com/) implementation using
[pulsar](https://github.com/quantmind/pulsar) asynchronous framework.

Requirements
---------------

* Python 3.4 or above
* [pulsar](https://github.com/quantmind/pulsar) 3.4 or above


Usage
---------------

Create a pusher instance::

    from pusher import Pusher

    pusher = Pusher(app_id, key, secret)