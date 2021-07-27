import logging
import threading
from pydoc import locate
from time import sleep

from octopus.settings import SEND_MIDDLEWARES

LOG = logging.getLogger('octopus')


class SenderThread(threading.Thread):
    """
    sender process
    """

    def __init__(self, process_queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.process_queue = process_queue

    def run(self):
        while True:
            try:
                line = self.process_queue.get_queue()
                for k, v in SEND_MIDDLEWARES.items():
                    obj = locate(k)()
                    obj.send(line)
            except Exception as e:
                LOG.error(e)
            sleep(0.01)
