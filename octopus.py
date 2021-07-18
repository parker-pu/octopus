#!/usr/bin/env python
import logging
import os
import sys

from octopus.comm.gen_collector import ReaderThread, main_loop
from octopus.comm.queue import DataQueue
from octopus.comm.send import SenderThread
from octopus.settings import BASE_DIR

LOG = logging.getLogger('octopus')


def write_pid(pid_file):
    """保存pid"""
    f = open(pid_file, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()


def main(argv):
    # 写入进程ID
    write_pid("{}/octopus.pid".format(BASE_DIR))

    reader = ReaderThread(
        300, 6000, False, ""
    )
    reader.start()

    # 启动发送中间件
    sender = SenderThread(reader)
    sender.start()
    LOG.info('SenderThread startup complete')

    # 启动收集脚本
    sys.stdin.close()
    main_loop()

    LOG.debug('Shutting down -- joining the reader thread.')
    reader.join()
    LOG.debug('Shutting down -- joining the sender thread.')
    sender.join()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
