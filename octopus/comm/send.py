#!/usr/bin/env python
import logging
import threading
from pydoc import locate
from time import sleep

from octopus.settings import SEND_MIDDLEWARES

LOG = logging.getLogger('octopus')


class SenderThread(threading.Thread):
    """
    数据发送
    """

    def __init__(self, reader, *args, **kwargs):
        """
        消费数据进行发送
        :param reader:
        :param args:
        :param kwargs:
        """
        super(SenderThread, self).__init__()
        self.reader = reader

    def run(self):
        while True:
            try:
                line = self.reader.read_rq.get(5)
                for k, v in SEND_MIDDLEWARES.items():
                    obj = locate(k)()
                    obj.send(line)
            except Exception as e:
                LOG.error(e)
            sleep(0.5)
