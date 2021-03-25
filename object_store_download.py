from pathlib import Path
from typing import Optional
from object_store import ObjectStore
import logging

import gevent
from gevent import monkey
from gevent.pool import Pool
from minio.error import ResponseError
from tqdm import tqdm

import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import List, Optional



ACCESS_KEY = "ACCESS_KEY"
HOST = "HOST"
SECRET_KEY = "SECRET_KEY"
BUCKET_NAME = "BUCKET_NAME"
DESTINATION = Path(".")
POOL_SIZE = 16

def download_file(file: str, bucket_name: str, minio_client, destination: Path):
    """Return the binary data of a file from the minio."""
    try:
        response = minio_client.get_object(bucket_name, file)

    except ResponseError as err:
        print(err)
        return
    logging.info(f"writing data to {destination}")
    destination.write_bytes(response.data)
    logging.info(f"releasing memory")
    del response

def download_data(file: str, bucket_name: str, dest: Path):
    """Parallel batching of data."""

    destination = dest / Path(file)
    if destination.exists():
        logging.info(f"file already exists")
        return
    logging.info(f"preparing data from {file}")
    store = ObjectStore()  # NOTE: Object store client is not thread safe.
    # We need to instantiate it in each unit of parallelization.
    store.connect()
    gevent.sleep(0.001)
    download_file(file, bucket_name, store.client, destination)

def stream_files(files, bucket_name: str, dest: Path, pool_size: int = 16):
    """Stream all files."""

    store = ObjectStore(HOST, ACCESS_KEY, SECRET_KEY)
    store.connect()

    logging.info(f"starting a stream to download the data with {pool_size} workers")

    pool = Pool(pool_size + 1)

    jobs = [pool.spawn(download_data, file, dest) for file in files]

    gevent.joinall(jobs)

def get_filename_generator(bucket_name: str):
    """Get a filename generator for a bucket."""
    store = ObjectStore(HOST, ACCESS_KEY, SECRET_KEY)
    store.connect()
    server_files = store.client.list_objects_v2(
        bucket_name, prefix="", recursive=True, start_after=""
    )

    def gen_filenames(files):
        for file in files:
            yield file.object_name

    return gen_filenames(server_files)

def download_store(bucket_name: str, dest: Path, pool_size: int = 16):
    """Download all the lzma files from the mino."""

    dest.mkdir(parents=True, exist_ok=True)

    gen = get_filename_generator(bucket_name)

    stream_files(gen, bucket_name, dest, pool_size=pool_size)

if __name__ == '__main__':

	download_store(bucket_name=BUCKET_NAME, dest=DESTINATION, pool_size= POOL_SIZE)
