class RetryException(Exception):
    """Signal that a process need to reconnect and retry."""
    pass


class TimeoutException(Exception):
    pass


class AbstractConnection(object):

    def connect(self):
        """Connect to the broker. """

    def close(self):
        """Close this instance of the connection. """

    def create_queue(self, name=None):
        """Create a queue.

        This method should be idempotent. Do nothing if the queue is
        already created.

        :type name: str
        :rtype: queue
        """

    def create_local_queue(self):
        """Create local queue.

        Create a temporary queue private to this
        connection.

        :rtype: queue
        """

    def create_pubsub(self, name):
        """Create a pubsub endpoint.

        This method should be idempotent. Do nothing if the endpoint is
        already created.

        :type name: str
        :rtype: pubsub
        """

    def delete_queue(self, queue):
        """Delete the queue in the broker. """

    def delete_pubsub(self, pubsub):
        """Delete the pubsub in the broker. """

    def subscribe(self, queue, pubsub):
        """Subscribe queue to pubsub.

        Messages in the pubsub are delivered to queue.

        :type queue: queue
        :type pubsub: pubsub
        """

    def publish(self, msg, pubsub):
        """Publish message in pubsub.

        :type msg: T
        :type pubsub: pubsub
        """

    def get(self, queue, timeout=float('inf')):
        """Blocking get.

        :type queue: queue
        :rtype: T
        """

    def put(self, msg, queue):
        """Non-blocking put.

        :type msg: T
        :type queue: queue
        """

    def retrying(self, f):
        """A wrapper for functions that need retrying on errors.

        Raise RetryException to rebuild connection and retry.

        :type f: Callable
        """

