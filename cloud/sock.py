import asyncio
import socket
from functools import partial

from pulsar.apps.greenio import wait



def create_connection(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                      source_address=None, socket_options=None):
    return wait(_create_connection())


def _create_connection(address, timeout, source_address):
    loop = asyncio.get_event_loop()
    tr, pr = yield from loop.create_connection()


def wrap_poolmanager(poolmanager):
    poolmanager._new_pool = partial(_new_pool, poolmanager._new_pool)
    return poolmanager


def wrap_socket(sock):
    pass


def _new_pool(method, scheme, host, port):
    pool = method(scheme, host, port)
    pool._new_conn = partial(_new_conn, pool._new_conn)
    return pool


def _new_conn(method):
    conn = method()
    conn._create_connection = create_connection
    conn._new_conn = partial(_conn_new_conn, conn)
    return conn


def _conn_new_conn(self):
    """ Establish a socket connection and set nodelay settings on it.

    :return: New socket connection.
    """
    extra_kw = {}
    if self.source_address:
        extra_kw['source_address'] = self.source_address

    if self.socket_options:
        extra_kw['socket_options'] = self.socket_options

    return self._create_connection(
            (self.host, self.port), self.timeout, **extra_kw)
