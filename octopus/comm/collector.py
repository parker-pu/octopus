#!/usr/bin/env python
import logging
import signal
import time
from os import kill
from subprocess import Popen

LOG = logging.getLogger('octopus')


class Collector(object):
    """
    一个采集器对应一个执行的脚本
    """

    def __init__(self, collection_name, interval, file_name, m_time, last_spawn=0):
        """Construct a new Collector."""
        self.name = collection_name
        self.interval = interval
        self.file_name = file_name
        self.last_spawn = last_spawn
        self.proc: Popen = None
        self.next_kill = 0
        self.kill_state = 0
        self.dead = False
        self.m_time = m_time  # 文件的最近一次更改时间
        self.generation = int(time.time())
        self.buffer = ""
        self.data_lines = []
        # Maps (metric, tags) to (value, repeated, line, timestamp) where:
        #  value: Last value seen.
        #  repeated: boolean, whether the last value was seen more than once.
        #  line: The last line that was read from that collector.
        #  timestamp: Time at which we saw the value for the first time.
        # This dict is used to keep track of and remove duplicate values.
        # Since it might grow unbounded (in case we see many different
        # combinations of metrics and tags) someone needs to regularly call
        # evict_old_keys() to remove old entries.
        self.values = {}
        self.lines_sent = 0
        self.lines_received = 0
        self.lines_invalid = 0
        self.last_datapoint = int(time.time())  # 最后的数据时间

    @property
    def read(self):
        """Read bytes from our subprocess and store them in our temporary
           line storage buffer.  This needs to be non-blocking."""

        # we have to use a buffer because sometimes the collectors
        # will write out a bunch of data points at one time and we
        # get some weird sized chunk.  This read call is non-blocking.

        # now read stderr for log messages, we could buffer here but since
        # we're just logging the messages, I don't care to
        if self.proc.poll() is None:
            return []

        try:
            out = self.proc.stderr.read()
            if out:
                LOG.debug('reading %s got %d bytes on stderr', self.name, len(out))
                for line in out.splitlines():
                    LOG.warning('%s: %s', self.name, line)
        except Exception as e:
            LOG.exception('uncaught exception in stderr read {}'.format(e))

        # we have to use a buffer because sometimes the collectors will write
        # out a bunch of data points at one time and we get some weird sized
        # chunk.  This read call is non-blocking.
        try:
            self.buffer += self.proc.stdout.read()
            if len(self.buffer):
                LOG.debug('reading %s, buffer now %d bytes', self.name, len(self.buffer))
        except Exception as e:
            LOG.exception('uncaught exception in stdout read {}'.format(e))
            return

        # iterate for each line we have
        while self.buffer:
            idx = self.buffer.find('\n')
            if idx == -1:
                break

            # one full line is now found and we can pull it out of the buffer
            line = self.buffer[0:idx].strip()
            if line:
                self.data_lines.append(line)
                self.last_datapoint = int(time.time())
            self.buffer = self.buffer[idx + 1:]

    def collect(self):
        """Reads input from the collector and returns the lines up to whomever
           is calling us.  This is a generator that returns a line as it
           becomes available."""

        while self.proc is not None:
            self.read
            if not len(self.data_lines):
                return
            while len(self.data_lines):
                yield self.data_lines.pop(0)

    def shutdown(self):
        """Cleanly shut down the collector"""

        if not self.proc:
            return
        try:
            if self.proc.poll() is None:
                # kill(self.proc, signal.SIGKILL)
                for attempt in range(5):
                    if self.proc.poll() is not None:
                        self.proc = None
                        return
                    LOG.info('Waiting %ds for PID %d (%s) to exit...'
                             % (5 - attempt, self.proc.pid, self.name))
                    time.sleep(1)
                kill(self.proc, signal.SIGKILL)
                self.proc.wait()
        except Exception as e:
            # we really don't want to die as we're trying to exit gracefully
            LOG.exception('ignoring uncaught exception while shutting down {}'.format(e))

    def evict_old_keys(self, cut_off):
        """Remove old entries from the cache used to detect duplicate values.

        Args:
          cut_off: A UNIX timestamp.  Any value that's older than this will be
            removed from the cache.
        """
        for key in self.values.keys():
            __time = self.values[key][3]
            if __time < cut_off:
                del self.values[key]

    def to_json(self):
        """Expose collector information in JSON-serializable format."""
        result = {}
        for attr in ["name", "m_time", "last_spawn", "kill_state", "next_kill",
                     "lines_sent", "lines_received", "lines_invalid",
                     "last_datapoint", "dead"]:
            result[attr] = getattr(self, attr)
        return result
