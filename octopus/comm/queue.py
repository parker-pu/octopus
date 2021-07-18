import logging
import queue

LOG = logging.getLogger('octopus')


class DataQueue:
    """
    A Queue for the reader thread
    """
    pq = ""

    def __init__(self, maxsize=100000):
        self.pq = queue.Queue(maxsize)

    def put(self, value):
        """A nonblocking put, that simply logs and discards the value when the
           queue is full, and returns false if we dropped."""
        try:
            self.pq.put(value, False)
        except Exception as e:
            LOG.error("DROPPED LINE: %s", e)
            return False
        return True

    def get(self, key):
        return self.pq.get(key)
