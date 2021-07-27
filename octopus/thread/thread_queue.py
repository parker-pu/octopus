import logging
import multiprocessing
import queue
# from multiprocessing import Queue
from queue import Queue

LOG = logging.getLogger('octopus')


#
# class ProcessQueue:
#
#     def __init__(self, *args, **kwargs):
#         self.queue = Queue()
#         self.data_duplication_dict: dict = multiprocessing.Manager().dict()
#         # self.data_duplication_dict: dict = {}
#
#     def put_queue(self, obj_name, value, block=True, timeout=None):
#         """
#         input value to queue
#         :param obj_name:
#         :param value:
#         :param block:
#         :param timeout:
#         :return:
#         """
#         last_value = self.data_duplication_dict.get(obj_name)
#         if last_value == value:
#             return False
#         else:
#             self.data_duplication_dict[obj_name] = value
#             print("in --< {}".format(value))
#             self.queue.put(obj=value, block=block, timeout=timeout)
#
#     def get_queue(self, block=True, timeout=None):
#         """
#         get value to queue
#         :param block:
#         :param timeout:
#         :return:
#         """
#         self.queue.get(block=block, timeout=timeout)
#

class ThreadQueue:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance:
            return cls.__instance
        else:
            obj = super().__new__(cls, *args, **kwargs)
            cls.__instance = obj
            return cls.__instance

    def __init__(self, do_filter=True, *args, **kwargs):
        self.do_filter = do_filter
        self.queue = Queue(maxsize=10000)
        self.data_duplication_dict: dict = {}

    def put_queue(self, obj_name, value, block=True, timeout=None):
        """
        input value to queue
        :param obj_name:
        :param value:
        :param block:
        :param timeout:
        :return:
        """
        if self.do_filter:
            self.queue.put(value, block=block, timeout=timeout)
        else:
            """
            取到上一次的数据
            """
            last_value = self.data_duplication_dict.get(obj_name)
            if last_value == value:
                return False
            else:
                self.data_duplication_dict[obj_name] = value
                self.queue.put(value, block=block, timeout=timeout)

    def get_queue(self, block=True, timeout=None):
        """
        get value to queue
        :param block:
        :param timeout:
        :return:
        """
        return self.queue.get(block=block, timeout=timeout)
