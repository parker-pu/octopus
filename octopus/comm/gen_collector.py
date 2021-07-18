#!/usr/bin/env python
import logging
import os
import queue
import signal
import subprocess
import threading
import time
import fcntl
from os import kill

from octopus.comm.collector import Collector
from octopus.comm.queue import DataQueue
from octopus.settings import ALLOWED_INACTIVITY_TIME, REMOVE_INACTIVE_COLLECTORS, BASE_DIR

LOG = logging.getLogger('octopus')

# global variables.
COLLECTORS = {}
ALIVE = True
MAX_READ_QUEUE_SIZE = 100000
MAX_REASONABLE_TIMESTAMP = 2209212000

MAX_SENDQ_SIZE = 10000
MAX_READQ_SIZE = 100000


def register_collector(collector):
    """
    Register a collector with the COLLECTORS global
    :param collector: Collector
    :return:
    """

    assert isinstance(collector, Collector), "collector=%r" % (collector,)
    # store it in the global list and initiate a kill for anybody with the
    # same name that happens to still be hanging around
    if collector.name in COLLECTORS:
        col = COLLECTORS[collector.name]
        if col.proc is not None:
            LOG.error('%s still has a process (pid=%d) and is being reset,'
                      ' terminating', col.name, col.proc.pid)
            col.shutdown()

    COLLECTORS[collector.name] = collector


def all_collectors():
    """Generator to return all collectors."""
    return COLLECTORS.values()


# collectors that are not marked dead
def all_valid_collectors():
    """Generator to return all defined collectors that haven't been marked
       dead in the past hour, allowing temporarily broken collectors a
       chance at redemption."""

    now = int(time.time())
    for col in all_collectors():
        if not col.dead or (now - col.last_spawn > 3600):
            yield col


# collectors that have a process attached (currenty alive)
def all_living_collectors():
    """Generator to return all defined collectors that have
       an active process."""

    for col in all_collectors():
        if col.proc is not None:
            yield col


def populate_collectors(collector_dir):
    """
    查找内部的收集器,同时更新收集器
    :param collector_dir: 收集器文件目录
    :return:
    """
    # get numerics from scriptdir, we're only setup to handle numeric paths
    # which define intervals for our monitoring scripts
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
                if collector_name in COLLECTORS:
                    col = COLLECTORS[collector_name]

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
                            register_collector(
                                Collector(collector_name, interval, file_name, m_time))
                else:
                    register_collector(
                        Collector(collector_name, interval, file_name, m_time))
            else:
                raise Exception("Permission denied")

    # 如果扫描到的版本与新版本相差30S以上,那么就从新版本删除
    to_delete = []
    for col in all_collectors():
        if col.generation < int(time.time() - 30):
            LOG.info('collector %s removed from the filesystem, forgetting', col.name)
            col.shutdown()
            to_delete.append(col.name)
    for name in to_delete:
        del COLLECTORS[name]


class ReaderThread(threading.Thread):
    """
    启动收集器的进程,从中读取数据
    The main ReaderThread is responsible for reading from the collectors
    and assuring that we always read from the input no matter what.
    All data read is put into the self.readerq Queue, which is
    consumed by the SenderThread.
   """
    read_rq: queue.Queue = ""

    def __init__(self, dedupinterval, evictinterval, deduponlyzero, ns_prefix=""):
        """Constructor.
            Args:
              dedupinterval: If a metric sends the same value over successive
                intervals, suppress sending the same value to the TSD until
                this many seconds have elapsed.  This helps graphs over narrow
                time ranges still see timeseries with suppressed datapoints.
              evictinterval: In order to implement the behavior above, the
                code needs to keep track of the last value seen for each
                combination of (metric, tags).  Values older than
                evictinterval will be removed from the cache to save RAM.
                Invariant: evictinterval > dedupinterval
              deduponlyzero: do the above only for 0 values.
              ns_prefix: Prefix to add to metric tags.
        """
        assert evictinterval > dedupinterval, "%r <= %r" % (evictinterval,
                                                            dedupinterval)
        super(ReaderThread, self).__init__()

        self.read_rq = DataQueue(maxsize=MAX_READ_QUEUE_SIZE)
        self.lines_collected = 0
        self.lines_dropped = 0
        self.dedupinterval = dedupinterval
        self.evictinterval = evictinterval
        self.deduponlyzero = deduponlyzero
        self.ns_prefix = ns_prefix

    def run(self):
        """Main loop for this thread.  Just reads from collectors,
           does our input processing and de-duping, and puts the data
           into the queue."""

        LOG.debug("ReaderThread up and running")

        last_evict_time = 0
        # we loop every second for now.  ideally we'll setup some
        # select or other thing to wait for input on our children,
        # while breaking out every once in a while to setup selects
        # on new children.
        while ALIVE:
            alc = all_living_collectors()
            for col in alc:
                for line in col.collect():
                    self.process_line(col, line)

            if self.dedupinterval != 0:  # if 0 we do not use dedup
                now = int(time.time())
                if now - last_evict_time > self.evictinterval:
                    last_evict_time = now
                    now -= self.evictinterval
                    for col in all_collectors():
                        col.evict_old_keys(now)

            # and here is the loop that we really should get rid of, this
            # just prevents us from spinning right now
            time.sleep(1)

    def process_line(self, col, line):
        """Parses the given line and appends the result to the reader queue.
        处理数据并添加到阅读队列
        """
        col.lines_sent += 1
        if not self.read_rq.put(line):
            self.lines_dropped += 1


def reap_children():
    """
    维护子进程
    """

    for col in all_living_collectors():
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
            register_collector(
                Collector(col.name, col.interval, col.file_name, col.m_time, col.last_spawn))


def check_children():
    """
        检测子进程，如果子进程心跳没有存活，那么久重新启动子进程
    :return:
    """

    for col in all_living_collectors():
        now = int(time.time())

        if (col.interval == 0) and (col.last_datapoint < (now - ALLOWED_INACTIVITY_TIME)):
            # It's too old, kill it
            LOG.warning('Terminating collector %s after %d seconds of inactivity',
                        col.name, now - col.last_datapoint)
            col.shutdown()
            if not REMOVE_INACTIVE_COLLECTORS:
                register_collector(Collector(col.name, col.interval, col.file_name,
                                             col.m_time, col.last_spawn))


def set_nonblocking(fd):
    """Sets the given file descriptor to non-blocking mode."""
    fl = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, fl)


def spawn_collector(col):
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
    set_nonblocking(col.proc.stdout.fileno())
    set_nonblocking(col.proc.stderr.fileno())
    if col.proc.pid > 0:
        col.dead = False
        LOG.info('spawned %s (pid=%d)', col.name, col.proc.pid)
        return
    # FIXME: handle errors better
    LOG.error('failed to spawn collector: %s', col.file_name)


def spawn_children():
    """
    执行收集器
    :return:
    """

    if not ALIVE:
        return

    for col in all_valid_collectors():
        now = int(time.time())
        if col.interval == 0:
            if col.proc is None:
                spawn_collector(col)
        elif col.interval <= now - col.last_spawn:
            if col.proc is None:
                spawn_collector(col)
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


def main_loop():
    """The main loop of the program that runs when we're not in stdin mode."""
    next_heartbeat = int(time.time() + 600)
    while ALIVE:
        populate_collectors("{}/collectors".format(BASE_DIR))  # 载入采集器脚本
        reap_children()  # 维护子进程
        check_children()
        spawn_children()  # 执行收集器
        time.sleep(3)
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600
