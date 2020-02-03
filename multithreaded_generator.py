"""Threaded generator.

    This is an example to split the work of a heavy function on a generator into mulitple processes.

    Unfortunately 'collect_q' cannot be defined with the __init__ of the class.
"""

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Queue, Process, cpu_count
from time import sleep
import numpy as np

def slow_func(i):
    print('Get data {}'.format(i))
    sleep(1)
    return i

def do_func(i):
    print('Process data {}'.format(i))
    sleep(0.25)


class ParPipeline(object):
    """Parallel pipeline example."""
    collect_q: Queue = Queue(maxsize=cpu_count())

    def __init__(self, gen, func, max_workers=cpu_count()):
        self.max_workers = max_workers
        self.gen = gen
        self.func = func
    def do_func(self, *arg, **kwargs):
        """Long running operation."""
        obj = self.func(*arg, **kwargs)
        self.collect_q.put(obj)
    def process(self):
        """Process work on the pool."""
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self.do_func, self.gen)
    def __iter__(self):
        """Fork processing on the pool to its own process."""
        daemon_thread = Process(target=self.process)
        daemon_thread.start()
        for _ in range(len(self)):
            yield self.collect_q.get()
    def __len__(self):
        return len(self.gen)




if __name__ == "__main__":
    l = np.arange(30)
    pipeline = ParPipeline(l, slow_func)
    for item in pipeline:
        do_func(item)