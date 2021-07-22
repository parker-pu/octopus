import logging
import multiprocessing
from multiprocessing import Queue

LOG = logging.getLogger('octopus')


class ProcessQueue:

    def __init__(self, *args, **kwargs):
        self.queue = Queue()
        self.data_duplication_dict: dict = multiprocessing.Manager().dict()
        # self.data_duplication_dict: dict = {}

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
            self.queue.put(obj=value, block=block, timeout=timeout)

    def get_queue(self, block=True, timeout=None):
        """
        get value to queue
        :param block:
        :param timeout:
        :return:
        """
        self.queue.get(block=block, timeout=timeout)
