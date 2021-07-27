#!/usr/bin/env python
import logging
import multiprocessing
import os
import sys
import time

from octopus.comm.children_collector import ChildrenCollector
from octopus.process.process_queue import ProcessQueue
from octopus.process.process_read import ReadProcess
from octopus.process.process_sender import SenderProcess
from octopus.thread.thread_queue import ThreadQueue
from octopus.settings import BASE_DIR
from octopus.thread.thread_read import ReadThread
from octopus.thread.thread_sender import SenderThread

LOG = logging.getLogger('octopus')


def write_pid(pid_file):
    """保存pid"""
    f = open(pid_file, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()


def thread_main(argv):
    # 写入进程ID
    write_pid("{}/octopus.pid".format(BASE_DIR))

    __col_dict: dict = {}
    thread_queue = ThreadQueue()
    thread_list = [
        ReadThread(thread_queue, __col_dict),
        SenderThread(thread_queue)
    ]

    # 启动阅读线程与发送线程
    for p in thread_list:
        p.start()

    # 扫描采集器
    def scan_collection():
        cc = ChildrenCollector(collection_dict=__col_dict)
        while True:
            cc.populate_collectors("{}/collectors".format(BASE_DIR))  # 载入采集器
            cc.reap_children()  # 维护子采集器
            cc.check_children()  # 检测子采集器
            cc.spawn_children()  # 执行收集器
            time.sleep(1)  # 1S扫描一次采集器

    scan_collection()

    for p in thread_list:
        p.join()


def process_main(argv):
    # 写入进程ID
    write_pid("{}/octopus.pid".format(BASE_DIR))

    process_queue = ProcessQueue()  # 进程间通信的队列
    collection_dict = multiprocessing.Manager().dict()  # 采集器字典

    process_list = [
        ReadProcess(process_queue, collection_dict),
        SenderProcess(process_queue)
    ]

    # 启动阅读线程与发送线程
    for p in process_list:
        p.start()

    # 扫描采集器
    def scan_collection():
        cc = ChildrenCollector(collection_dict=collection_dict)
        while True:
            cc.populate_collectors("{}/collectors".format(BASE_DIR))  # 载入采集器
            cc.reap_children()  # 维护子采集器
            cc.check_children()  # 检测子采集器
            cc.spawn_children()  # 执行收集器
            time.sleep(0.1)  # 1S扫描一次采集器

    scan_collection()

    for p in process_list:
        p.join()


if __name__ == '__main__':
    sys.exit(thread_main(sys.argv))
