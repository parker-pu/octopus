import logging
import multiprocessing
import queue

from multiprocessing import Queue

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


mgr = multiprocessing.Manager()


class ProcessQueue(Queue):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_duplication_dict = mgr.dict()

    def put_queue(self, obj_name, value, block=True, timeout=None):
        """
        input value to queue
        :param obj_name:
        :param value:
        :param block:
        :param timeout:
        :return:
        """
        last_value = self.data_duplication_dict.get(obj_name)
        if last_value == value:
            return False
        else:
            self.data_duplication_dict[obj_name] = value
            super().put(obj=value, block=block, timeout=timeout)

    def get_queue(self, block=True, timeout=None):
        """
        get value to queue
        :param block:
        :param timeout:
        :return:
        """
        super().get(block=block, timeout=timeout)
