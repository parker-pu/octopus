# -*- coding: utf-8 -*-
import fcntl
import logging
import multiprocessing
import os
import signal
import subprocess
import time

from os import kill
from octopus.comm.collector import Collector
from octopus.settings import ALLOWED_INACTIVITY_TIME, REMOVE_INACTIVE_COLLECTORS, ALIVE

LOG = logging.getLogger('octopus')


class ChildrenCollector:
    """
    children collector
    """

    def __init__(self, collection_dict):
        self.collection_dict = collection_dict

    def populate_collectors(self, collector_dir):
        """
        查找内部的收集器,同时更新收集器
        """
        for interval in os.listdir(collector_dir):
            if not interval.isdigit():  # 判断是否是数字
                continue
            interval = int(interval)

            """
            找到具体的采集器的名称
            """
            for collector_name in os.listdir('%s/%d' % (collector_dir, interval)):
                if collector_name.startswith('.'):
                    continue

                file_name = '%s/%d/%s' % (collector_dir, interval, collector_name)
                if os.path.isfile(file_name) and os.access(file_name, os.X_OK):
                    m_time = os.path.getmtime(file_name)  # 文件的最近更新时间
                    """
                    （1）如果采集器存在，那么就判断采集器的执行时间是否相等，执行时间不想等就直接抛出错误后继续，
                    再次判断是否是更新过，如果更新过那就检查采集器是否有运行时间，没有运行时间就重新载入采集器
                    （2）如果采集器不存在，那么就载入采集器
                    """
                    if collector_name in self.collection_dict.keys():
                        col = self.collection_dict.get(collector_name)

                        if col.interval != interval:
                            LOG.error('two collectors with the same name %s and '
                                      'different intervals %d and %d',
                                      collector_name, interval, col.interval)
                            continue

                        col.generation = int(time.time())
                        if col.m_time < m_time:
                            LOG.info('%s has been updated on disk', col.name)
                            col.m_time = m_time

                            # 如果采集器没有运行时间,那么就重新运行采集器
                            if not col.interval:
                                col.shutdown()
                                LOG.info('Respawning %s', col.name)
                                self.register_collector(
                                    Collector(collector_name, interval, file_name, m_time))
                    else:
                        print("开始注册")
                        self.register_collector(
                            Collector(collector_name, interval, file_name, m_time))
                else:
                    raise Exception("Permission denied")

        # 如果扫描到的版本与新版本相差30S以上,那么就从新版本删除
        # TODO:这个方法还需要考虑
        to_delete = []
        for col in self.collection_dict.values():
            if col.generation < int(time.time() - 30):
                LOG.info('collector %s removed from the filesystem, forgetting', col.name)
                col.shutdown()
                to_delete.append(col.name)
        for name in to_delete:
            del self.collection_dict[name]

    def register_collector(self, collector):
        """
        Register a collector with the COLLECTORS global
        :param collector: Collector
        :return:
        """

        assert isinstance(collector, Collector), "collector=%r" % (collector,)
        # store it in the global list and initiate a kill for anybody with the
        # same name that happens to still be hanging around
        if collector.name in self.collection_dict.keys():
            col = self.collection_dict.get(collector.name)
            if col.proc is not None:
                LOG.error('%s still has a process (pid=%d) and is being reset,'
                          ' terminating', col.name, col.proc.pid)
                col.shutdown()

        self.collection_dict[collector.name] = collector
        print(collector.name)
        print(self.collection_dict.keys())
        print(self.collection_dict.values())

    def all_living_collectors(self):
        """Generator to return all defined collectors that have
           an active process."""

        for col in self.collection_dict.values():
            if col.proc is not None:
                yield col

    def reap_children(self):
        """
        维护子进程
        """
        for col in self.all_living_collectors():
            now = int(time.time())
            # FIXME: this is not robust.  the asyncproc module joins on the
            # reader threads when you wait if that process has died.  this can cause
            # slow dying processes to hold up the main loop.  good for now though.
            status = col.proc.poll()
            if status is None:
                continue
            col.proc = None

            # behavior based on status.  a code 0 is normal termination, code 13
            # is used to indicate that we don't want to restart this collector.
            # any other status code is an error and is logged.
            if status == 13:
                LOG.info('removing %s from the list of collectors (by request)',
                         col.name)
                col.dead = True
            elif status != 0:
                LOG.warning('collector %s terminated after %d seconds with '
                            'status code %d, marking dead',
                            col.name, now - col.last_spawn, status)
                col.dead = True
            else:
                self.register_collector(
                    Collector(col.name, col.interval, col.file_name, col.m_time, col.last_spawn))

    def check_children(self):
        """
        检测子进程，如果子进程心跳没有存活，那么久重新启动子进程，主要针对长期运行的程序
        """

        for col in self.all_living_collectors():
            now = int(time.time())

            # 最后的检查时间是在设置的时间之前,那么就关掉这个任务,让后重启
            if (col.interval == 0) and (col.last_datapoint < (now - ALLOWED_INACTIVITY_TIME)):
                # It's too old, kill it
                LOG.warning('Terminating collector %s after %d seconds of inactivity',
                            col.name, now - col.last_datapoint)
                col.shutdown()
                if not REMOVE_INACTIVE_COLLECTORS:
                    self.register_collector(
                        Collector(col.name, col.interval, col.file_name, col.m_time, col.last_spawn))

    def spawn_children(self):
        """
        执行收集器
        :return:
        """

        if not ALIVE:
            return

        for col in self.collection_dict.values():
            now = int(time.time())
            if col.interval == 0:
                if col.proc is None:
                    self.spawn_collector(col)
            elif col.interval <= now - col.last_spawn:
                if col.proc is None:
                    self.spawn_collector(col)
                    continue

                # I'm not very satisfied with this path.  It seems fragile and
                # overly complex, maybe we should just reply on the asyncproc
                # terminate method, but that would make the main tcollector
                # block until it dies... :|
                if col.next_kill > now:
                    continue
                if col.kill_state == 0:
                    LOG.warning('warning: %s (interval=%d, pid=%d) overstayed '
                                'its welcome, SIGTERM sent',
                                col.name, col.interval, col.proc.pid)
                    kill(col.proc, signal.SIGKILL)
                    col.next_kill = now + 5
                    col.kill_state = 1
                elif col.kill_state == 1:
                    LOG.error('error: %s (interval=%d, pid=%d) still not dead, '
                              'SIGKILL sent',
                              col.name, col.interval, col.proc.pid)
                    kill(col.proc, signal.SIGKILL)
                    col.next_kill = now + 5
                    col.kill_state = 2
                else:
                    LOG.error('error: %s (interval=%d, pid=%d) needs manual '
                              'intervention to kill it',
                              col.name, col.interval, col.proc.pid)
                    col.next_kill = now + 300

    def spawn_collector(self, col):
        """Takes a Collector object and creates a process for it."""

        LOG.info('%s (interval=%d) needs to be spawned', col.name, col.interval)
        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "close_fds": True,
            "preexec_fn": os.setsid,
            "encoding": "utf-8",
        }

        try:
            col.proc = subprocess.Popen(col.file_name, **kwargs)
            print("----> {}".format(col.proc))
            print("cc----> {}".format(col))
        except OSError as e:
            LOG.error('Failed to spawn collector %s: %s' % (col.file_name, e))
            return
        # The following line needs to move below this line because it is used in
        # other logic and it makes no sense to update the last spawn time if the
        # collector didn't actually start.
        col.last_spawn = int(time.time())
        # Without setting last_datapoint here, a long running check (>15s) will be
        # killed by check_children() the first time check_children is called.
        col.last_datapoint = col.last_spawn
        self.set_nonblocking(col.proc.stdout.fileno())
        self.set_nonblocking(col.proc.stderr.fileno())
        if col.proc.pid > 0:
            col.dead = False
            LOG.info('spawned %s (pid=%d)', col.name, col.proc.pid)
            return
        # FIXME: handle errors better
        LOG.error('failed to spawn collector: %s', col.file_name)

    @staticmethod
    def set_nonblocking(fd):
        """Sets the given file descriptor to non-blocking mode."""
        fl = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, fl)
