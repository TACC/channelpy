from .base_connection import AbstractConnection, RetryException

import rabbitpy
import rabbitpy.exceptions

from .exceptions import ChannelClosedException


class RabbitConnection(AbstractConnection):

    def __init__(self, uri):
        self._uri = uri or 'amqp://127.0.0.1:5672'

        self._conn = None
        self._ch = None

    def connect(self):
        """Connect to the broker. """

        self._conn = rabbitpy.Connection(self._uri)
        self._ch = self._conn.channel()
        self._ch.prefetch_count(value=1, all_channels=True)

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
        return _queue

    def create_local_queue(self):
        """Create a local queue

        :rtype: queue
        """
        _queue = rabbitpy.Queue(self._ch, exclusive=True)
        _queue.declare()
        return _queue

    def create_pubsub(self, name):
        """Create a fanout exchange.

        :type name: str
        :rtype: pubsub
        """
        _exchange = rabbitpy.FanoutExchange(self._ch, name, durable=True)
        _exchange.declare()
        return _exchange

    def subscribe(self, queue, pubsub):
        queue.bind(pubsub)

    def publish(self, msg, pubsub):
        _msg = rabbitpy.Message(self._ch, msg)
        _msg.publish(pubsub, '')

    def delete_queue(self, queue):
        try:
            queue.delete()
        except rabbitpy.exceptions.RabbitpyException:
            pass

    def delete_pubsub(self, pubsub):
        try:
            pubsub.delete()
        except rabbitpy.exceptions.RabbitpyException:
            pass

    def get(self, queue):
        """Non-blocking get.  Return None if empty.

        :type queue: queue
        :rtype: Optional[T]
        """
        _msg = queue.get()
        if _msg is None:
            return None
        _msg.ack()
        return _msg.body

    def put(self, msg, queue):
        """Non-blocking put.

        :type msg: T
        :type queue: queue
        """
        _msg = rabbitpy.Message(self._ch, msg, {})
        _msg.publish('', queue.name)

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
