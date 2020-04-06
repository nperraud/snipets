"""Threaded generator.

    This is an example to transform a generator into a separate process.

    This is useful if your generator take some time. As an example, it can be
    ued to loads data from the disk and does some preprocessing.
"""

import multiprocessing as mp
from time import sleep


def get_data(l=10):
    for i in range(l):
        print('Get data {}'.format(i))
        sleep(0.5)
        yield i


def do_func(i):
    print('Process data {}'.format(i))
    sleep(1)


def queued_generator(g, maxsize=2):
    """Transform a generator in a threaded generator."""
    import multiprocessing as mp

    q: mp.Queue[Type] = mp.Queue(maxsize=maxsize)

    def put_data_in_queue(q, g):
        try:
            for d in g:
                q.put(d)
        except Exception as _:
            import traceback
            traceback.print_exc()
            print("Error in the generator")
        q.put("DONE")

    reader_p = mp.Process(target=put_data_in_queue, args=(q, g))
    #     reader_p.daemon = True
    reader_p.start()  # Launch reader_proc() as a separate python process

    while True:
        el = q.get()
        if el == "DONE":
            return
        else:
            yield el



if __name__ == "__main__":
    my_gen = queued_generator(get_data(10), maxsize=4)
    for d in my_gen:
        do_func(d)
