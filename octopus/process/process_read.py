import logging
import time
from multiprocessing import Process

from octopus.comm.collector import Collector
from octopus.process.process_queue import ProcessQueue
from octopus.settings import ALIVE

LOG = logging.getLogger('octopus')


class ReadProcess(Process):
    """
    Read process
    """

    def __init__(self, process_queue, collection_dict, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)

        self.process_queue: ProcessQueue = process_queue
        self.collection_dict = collection_dict
        self.lines_collected = 0
        self.lines_dropped = 0
        self.dedupinterval = 300
        self.evictinterval = 600
        self.deduponlyzero = False
        self.ns_prefix = ""

    def all_living_collectors(self):
        """Generator to return all defined collectors that have
           an active process."""
        for col in self.collection_dict.values():
            if col.proc is not None:
                yield col

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
            for col in self.all_living_collectors():
                for line in col.collect():
                    self.process_line(col, line)

            if self.dedupinterval != 0:  # if 0 we do not use dedup
                now = int(time.time())
                if now - last_evict_time > self.evictinterval:
                    last_evict_time = now
                    now -= self.evictinterval
                    for col in self.collection_dict.values():
                        col.evict_old_keys(now)

            # and here is the loop that we really should get rid of, this
            # just prevents us from spinning right now
            time.sleep(1)

    def process_line(self, col: Collector, line):
        """Parses the given line and appends the result to the reader queue.
        处理数据并添加到阅读队列
        """
        col.lines_sent += 1
        if not self.process_queue.put_queue(col.name, line):
            self.lines_dropped += 1
