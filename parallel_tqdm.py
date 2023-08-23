from tqdm.contrib.concurrent import process_map

from time import sleep
from multiprocessing import cpu_count
def do_stuff(x):
    sleep(0.1)
    return x + 1

# r = []
# for i in tqdm(range(100)):
#     r.append(do_stuff(i))

ncpu = cpu_count()

r = process_map(do_stuff, list(range(100)), max_workers=ncpu )
print(r)
