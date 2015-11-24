import asyncio
from functools import partial

from botocore.vendored.requests.packages.urllib3.util.ssl_ import (
    resolve_cert_reqs, resolve_ssl_version, create_urllib3_context,
    assert_fingerprint, ssl)
from botocore.vendored.requests.packages.urllib3.packages.ssl_match_hostname\
    import match_hostname

from pulsar.apps.greenio import wait


def wrap_poolmanager(poolmanager):
    poolmanager._new_pool = partial(_new_pool, poolmanager._new_pool)
    return poolmanager


class Sock(asyncio.StreamReaderProtocol):
    '''Protocol for greenlet friendly sockets
    '''
    _timeout = None

    def __init__(self):
        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader(loop=loop)
        super().__init__(reader, loop=loop, client_connected_cb=_pass)

    def __getattr__(self, name):
        return getattr(self._sock, name)

    def __repr__(self):
        if self._sock:
            return repr(self._sock)
        else:
            return self.__class__.__name__
    __str__ = __repr__

    @property
    def _sock(self):
        if self._stream_writer:
            return self._stream_writer._transport._sock

    def fileno(self):
        if self._sock:
            return self._sock.fileno()
        else:
            return 0

    def send(self, data):
        self._stream_writer.write(data)
        # return wait(self._stream_writer.drain())
    sendall = send

    def makefile(self, mode=None):
        return SockRead(self)

    def close(self):
        if self._stream_writer:
            self._stream_writer.close()

    def settimeout(self, timeout):
        self._timeout = timeout


class SockRead:
    '''Wrap a Sock object for green IO
    '''
    def __init__(self, sock):
        self._sock = sock

    def readline(self, len=0):
        return wait(self._sock._stream_reader.readline())

    def read(self, len=0):
        return wait(self._sock._stream_reader.read(len))

    def readinto(self, b):
        return wait(self._readinto(b))

    def flush(self):
        pass

    def close(self):
        pass

    def fileno(self):
        return self._sock.fileno()

    def _readinto(self, b):
        n = len(b)
        bytes = yield from self._sock._stream_reader.read(n)
        if bytes:
            n = len(bytes)
            b[0:n] = bytes
            return n
        return 0


class StreamingBodyWsgiIterator:
    '''A pulsar compliant WSGI iterator
    '''
    _data = None

    def __init__(self, body, blocking_call, n=-1):
        self.body = body
        self._blocking_call = blocking_call
        self.n = n

    def __iter__(self):

        while True:
            yield self._blocking_call(self._read_body)
            if not self._data:
                break

    def _read_body(self):
        self._data = self.body.read(self.n)
        return self._data


def _pass(*args):
    pass


def _new_pool(method, scheme, host, port):
    pool = method(scheme, host, port)
    pool._new_conn = partial(_new_conn, pool._new_conn)
    return pool


def _new_conn(method):
    conn = method()
    conn.connect = partial(_new_ssl_conn, conn)
    return conn


def _new_ssl_conn(self):
    """ Establish a socket connection and set nodelay settings on it.

    :return: New socket connection.
    """
    return wait(_ssl_connect(self))


def _ssl_connect(self):
    if getattr(self, '_tunnel_host', None):
        raise NotImplementedError

    cert_reqs = resolve_cert_reqs(self.cert_reqs)
    ssl_version = resolve_ssl_version(self.ssl_version)

    context = create_urllib3_context(ssl_version, cert_reqs)
    if self.ca_certs:
        context.load_verify_locations(self.ca_certs, None)

    if self.cert_file:
        context.load_cert_chain(self.cert_file, self.key_file)

    loop = asyncio.get_event_loop()
    _, sock = yield from loop.create_connection(
        Sock, self.host, self.port,
        ssl=context,
        local_addr=self.source_address)

    sock.settimeout(self.timeout)
    self.sock = sock

    if self.assert_fingerprint:
        assert_fingerprint(self.sock.getpeercert(binary_form=True),
                           self.assert_fingerprint)
    elif (cert_reqs != ssl.CERT_NONE and
            self.assert_hostname is not False):
        cert = self.sock.getpeercert()
        if not cert.get('subjectAltName', ()):
            # TODO: fix this hostname
            hostname = None
            match_hostname(cert, self.assert_hostname or hostname)

        self.is_verified = (cert_reqs == ssl.CERT_REQUIRED
                            or self.assert_fingerprint is not None)

    return self
