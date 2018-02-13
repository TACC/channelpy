import json
import time
import uuid
import os
import errno
import threading
import socket

import yaml

from .exceptions import *
from .base_connection import RetryException
from .rabbitpy_connection import RabbitConnection
from .connections import connections


CONFIG_FILE = '~/.channelpy.yml'


class BasicQueue(object):
    """
    Minimal queue functionality when events and pub/sub are not needed.
    """

    DEFAULT_RETRY_TIMEOUT = 10

    def __init__(self, name, connection=None, retry_timeout=None):
        self.name = name
        self.connection = connection
        self.retry_timeout = retry_timeout or self.DEFAULT_RETRY_TIMEOUT
        self._try_until_timeout(self._reconnect,
                                timeout=self.retry_timeout)()

    def _reconnect(self):
        self.connection.connect()
        self._queue = self.connection.create_queue(self.name)

    def close(self):
        """Close this instance of the channel."""

        def _close(this):
            this.connection.close()

        t = threading.Thread(target=_close, args=(self,))
        t.start()

    def delete(self):
        """Delete the queue completely."""

        def _delete(this):
            this.connection.delete_queue(this._queue)
            this.close()

        t = threading.Thread(target=_delete, args=(self,))
        t.start()

    def _try_until_timeout(self, f, timeout=10, sleep=0.01):
        _f = self.connection.retrying(f)

        def wrapper(*args, **kwargs):
            start = time.time()
            while True:
                try:
                    return _f(*args, **kwargs)
                except RetryException:
                    if time.time() - start > timeout:
                        raise ChannelTimeoutException()
                    time.sleep(sleep)

        return wrapper

    def _get(self):
        return self.connection.get(self._queue)

    def get(self):
        return self._retrying(self._get)()

    def _put(self, msg):
        self.connection.put(msg, self._queue)

    def put(self, msg):
        self._retrying(self._put)(msg)

    def _retrying(self, f):
        _f = self.connection.retrying(f)

        def wrapper(*args, **kwargs):
            try:
                return _f(*args, **kwargs)
            except RetryException:
                self._try_until_timeout(self._reconnect,
                                        timeout=self.retry_timeout)()
                return f(*args, **kwargs)

        return wrapper


class Queue(BasicQueue):

    DEFAULT_RETRY_TIMEOUT = 10

    def _reconnect(self):
        self.connection.connect()
        self._queue = self.connection.create_queue(self.name)
        self._event_queue = self.connection.create_local_queue()
        self._pubsub = self.connection.create_pubsub(self.name)
        self.connection.subscribe(self._event_queue, self._pubsub)

    def event(self, ev):
        """Publish an event."""

        self.connection.publish(ev, self._pubsub)

    def _check_for_events(self):
        ev = self.connection.get(self._event_queue)
        if ev is None:
            return
        obj = json.loads(ev.decode('utf-8'))
        if obj == 'close':
            raise ChannelCloseAllException()
        else:
            raise ChannelEventException(obj)

    def _try_until_timeout(self, f, timeout=10, sleep=0.01):
        _f = self.connection.retrying(f)

        def wrapper(*args, **kwargs):
            start = time.time()
            while True:
                try:
                    return _f(*args, **kwargs)
                except RetryException:
                    if time.time() - start > timeout:
                        raise ChannelTimeoutException()
                    time.sleep(sleep)

        return wrapper

    def _get(self):
        self._check_for_events()
        return self.connection.get(self._queue)

    def _put(self, msg):
        self._check_for_events()
        self.connection.put(msg, self._queue)

    def delete(self):
        """Delete the queue completely."""

        def _delete(this):
            this.connection.delete_queue(this._queue)
            this.connection.delete_pubsub(this._pubsub)
            this.close()

        t = threading.Thread(target=_delete, args=(self,))
        t.start()


class ChannelEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, Channel):
            return obj.to_json()
        return super(ChannelEncoder, self).default(obj)


def as_channel(dct):
    if '__channel__' in dct:
        return Channel.from_json(dct)

    return dct


def checking_events(f):

    def wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except ChannelCloseAllException:
            self.close()
            raise ChannelClosedException()

    return wrapper


class BaseChannel(object):

    POLL_FREQUENCY = 0.01  # seconds

    def __init__(self, name=None, rm=False,
                 connection_type=None,
                 retry_timeout=None,
                 **kwargs):
        """
        :type name: str
        :type rm: bool
        :type connection_type: AbstractConnection
        :type kwargs: Dict
        """
        self.name = name or uuid.uuid4().hex
        self.connection_type = RabbitConnection
        self.retry_timeout = retry_timeout
        self.connection_args = {}

        # try to resolve token via gethostbyname
        self._try_resolve_host()
        # try to read config from file
        self._try_config_from_file()
        # overwrite with env variables, if any
        self._try_env_variables()
        # overwrite with parameter, if any
        if connection_type is not None:
            self.connection_type = connection_type
        if self.connection_type is None:
            raise ChannelInitConnectionException(
                'no connection type found')

        self.connection_args.update(kwargs)
        self.connection = self.connection_type(**self.connection_args)
        self._rm = rm

    def to_json(self):
        return {
            '__channel__': True,
            'name': self.name,
            'connection_type': self.connection_type.__name__,
            'retry_timeout': self.retry_timeout,
            'connection_args': self.connection_args
        }

    @classmethod
    def from_json(cls, obj):
        class_name = obj['connection_type']
        conn_cls = connections[class_name]
        return cls(name=obj['name'],
                   connection_type=conn_cls,
                   retry_timeout=obj['retry_timeout'],
                   **obj['connection_args'])

    def _try_resolve_host(self):
        token = os.environ.get('CHANNELPY_RABBITMQ_HOSTNAME', 'rabbit')
        try:
            host = socket.gethostbyname(token)
            self.connection_args.update({'uri': 'amqp://{}:5672'.format(host)})
        except socket.error:
            pass

    def _try_env_variables(self):
        try:
            self.connection_type = connections[
                os.environ['CHANNELPY_CONNECTION_TYPE']]
        except KeyError:
            pass
        try:
            self.connection_args.update({'uri': os.environ['CHANNELPY_URI']})
        except KeyError:
            pass

    def _try_config_from_file(self):
        try:
            with open(os.path.expanduser(CONFIG_FILE)) as config:
                cfg = yaml.load(config)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                return
            raise
        if 'connection' in cfg:
            self.connection_type = connections[cfg['connection']]
        self.connection_args.update(cfg.get('arguments', {}))
        self.retry_timeout = cfg.get('retry_timeout', None)
        self.POLL_FREQUENCY = float(cfg.get('poll_frequency',
                                            self.POLL_FREQUENCY))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del exc_type, exc_val, exc_tb
        if self._rm:
            self.delete()
        else:
            self.close()

    def clone(self, name=None, rm=False, retry_timeout=None):
        return Channel(name=name,
                       rm=rm,
                       connection_type=self.connection_type,
                       retry_timeout=retry_timeout,
                       **self.connection_args)

    def dup(self):
        return self.clone(name=self.name, rm=True,
                          retry_timeout=self.retry_timeout)

    @checking_events
    def get(self, timeout=float('inf')):
        start = time.time()
        while True:
            if self._queue is None:
                raise ChannelClosedException()
            msg = self._queue.get()
            if msg is not None:
                return self._process(msg.decode('utf-8'))
            if time.time() - start > timeout:
                raise ChannelTimeoutException()
            time.sleep(self.POLL_FREQUENCY)

    @staticmethod
    def _process(msg):
        return json.loads(msg, object_hook=as_channel)

    @checking_events
    def put(self, value):
        if self._queue is None:
            raise ChannelClosedException()
        self._queue.put(
            json.dumps(value, cls=ChannelEncoder).encode('utf-8'))

    def put_sync(self, value, timeout=float('inf')):
        """Synchronous put.

        Wraps the object ``value`` in the form::

            {"value": value, "reply_to": ch}

        and waits for a response in ``ch``.

        """
        with Channel(connection_type=self.connection_type,
                     rm=True,
                     **self.connection_args) as ch:
            self.put({
                'value': value,
                'reply_to': ch
            })
            return ch.get(timeout=timeout)

    def close(self):
        if self._queue is None:
            raise ChannelClosedException()
        self._queue.close()
        self._queue = None

    def delete(self):
        if self._queue is None:
            raise ChannelClosedException()
        self._queue.delete()
        self.close()


class BasicChannel(BaseChannel):

    def __init__(self, name=None, rm=False,
                 connection_type=None,
                 retry_timeout=None,
                 **kwargs):
        super().__init__(name, rm, connection_type, retry_timeout, **kwargs)
        self._queue = BasicQueue(self.name, self.connection,
                                 retry_timeout=self.retry_timeout)


class Channel(BaseChannel):

    def __init__(self, name=None, rm=False,
                 connection_type=None,
                 retry_timeout=None,
                 **kwargs):
        """
        :type name: str
        :type rm: bool
        :type connection_type: AbstractConnection
        :type kwargs: Dict
        """
        super().__init__(name, rm, connection_type, retry_timeout, **kwargs)
        self._queue = Queue(self.name, self.connection,
                            retry_timeout=self.retry_timeout)

    def close_all(self):
        if self._queue is None:
            raise ChannelClosedException()
        self._queue.event(
            json.dumps('close').encode('utf-8'))
        self.close()

    def event(self, obj):
        if self._queue is None:
            raise ChannelClosedException()
        self._queue.event(json.dumps(obj).encode('utf-8'))

