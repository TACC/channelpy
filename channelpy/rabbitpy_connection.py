import queue
import threading

from .base_connection import AbstractConnection, RetryException
from .base_connection import TimeoutException

import rabbitpy
import rabbitpy.exceptions

from .exceptions import ChannelClosedException


def feed(python_queue, rabbit_queue):
    g = rabbit_queue.consume()
    for elt in g:
        python_queue.put(elt)


class TimeoutQueue(object):

    def __init__(self, rabbit_queue):
        self.rabbit_queue = rabbit_queue
        self.q = queue.Queue()
        self._thread = threading.Thread(
            target=feed, args=(self.q, self.rabbit_queue))
        self._thread.name = rabbit_queue.name
        self._thread.daemon = True
        self._thread.start()
        
    def get(self, timeout=float('inf')):
        if timeout == float('inf'):
            timeout = None
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutException()

    def put(self, value):
        pass


class RabbitConnection(AbstractConnection):

    def __init__(self, uri):
        self._uri = uri or 'amqp://127.0.0.1:5672'

        self._conn = None
        self._ch = None

    def connect(self):
        """Connect to the broker. """

        self._conn = rabbitpy.Connection(self._uri)
        self._ch = self._conn.channel()

    def close(self):
        """Close this instance of the connection. """

        self._ch.close()
        self._conn.close()

    def create_queue(self, name=None):
        """Create queue for messages.

        :type name: str
        :rtype: queue
        """
        _queue = rabbitpy.Queue(self._ch, name=name, durable=True)
        _queue.declare()
        return TimeoutQueue(_queue)

    def create_local_queue(self):
        """Create a local queue

        :rtype: queue
        """
        _queue = rabbitpy.Queue(self._ch, exclusive=True)
        _queue.declare()
        return TimeoutQueue(_queue)

    def create_pubsub(self, name):
        """Create a fanout exchange.

        :type name: str
        :rtype: pubsub
        """
        _exchange = rabbitpy.FanoutExchange(self._ch, name, durable=True)
        _exchange.declare()
        return _exchange

    def subscribe(self, queue, pubsub):
        queue.rabbit_queue.bind(pubsub)

    def publish(self, msg, pubsub):
        _msg = rabbitpy.Message(self._ch, msg)
        _msg.publish(pubsub, '')

    def delete_queue(self, queue):
        try:
            queue.rabbit_queue.delete()
        except rabbitpy.exceptions.RabbitpyException:
            pass

    def delete_pubsub(self, pubsub):
        try:
            pubsub.delete()
        except rabbitpy.exceptions.RabbitpyException:
            pass

    def get(self, queue, timeout=float('inf')):
        """Blocking get.

        :type queue: queue
        :rtype: T
        """
        _msg = queue.get(timeout=timeout)
        _msg.ack()
        return _msg.body

    def put(self, msg, queue):
        """Non-blocking put.

        :type msg: T
        :type queue: queue
        """
        _msg = rabbitpy.Message(self._ch, msg, {})
        _msg.publish('', routing_key=queue.rabbit_queue.name)

    def retrying(self, f):
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except rabbitpy.exceptions.AMQPNotFound:
                raise ChannelClosedException()
            except (rabbitpy.exceptions.AMQPException,
                    rabbitpy.exceptions.RabbitpyException):
                raise RetryException()
        return wrapper
