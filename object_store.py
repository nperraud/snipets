"""Minio object store."""
import contextlib
import logging
import sys
import tempfile
from pathlib import Path
from typing import Iterator, List, Optional

import gevent
import minio
import urllib3
from minio import Minio
from minio.error import ResponseError

# from predictor.config import (MINIO_ACCESS_KEY, MINIO_HOST, MINIO_SECRET_KEY,
#                               MINIO_SECURE)


class ObjectStore(object):
    """Object store."""

    def __init__(self,MINIO_HOST, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE: bool=False):
        """Construct store object."""
        self.client: Minio
        self.MINIO_ACCESS_KEY = MINIO_ACCESS_KEY
        self.MINIO_HOST = MINIO_HOST
        self.MINIO_SECRET_KEY = MINIO_SECRET_KEY
        self.MINIO_SECURE = MINIO_SECURE

    @staticmethod
    def get_client():
        """Connect to minio client."""
        logging.debug("connecting to server")
        return Minio(
            self.MINIO_HOST,
            access_key=self.MINIO_ACCESS_KEY.,
            secret_key=self.MINIO_SECRET_KEY,
            secure=self.MINIO_SECURE,
        )

    @property
    def is_connected(self) -> bool:
        """Check client connection."""
        return bool(self.client)

    def connect(self):
        """Connect to server."""
        try:
            self.client = ObjectStore.get_client()
        except minio.error.MinioError as err:
            logging.error(f"error connecting to server {err}")
            raise err

    def ensure_bucket(self, bucket_name: str):
        """Ensure the bucket exists."""
        if not self.client:
            raise minio.error.MinioError("client not connected")

        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def upload_blob(self, filepath: Path, bucket_name: str):
        """Upload a file blob."""
        try:
            logging.debug(f"uploading file")
            self.client.fput_object(bucket_name, filepath.name, filepath)
            logging.debug(f"uploaded file")

        except minio.error.MinioError as err:
            logging.error(f"exception uploading: {err}")

    def download_blob_response(
        self, object_name: str, bucket_name: str
    ) -> urllib3.response.HTTPResponse:
        """Download a file blob."""
        logging.debug(f"downloading blob: {object_name}")

        try:
            return self.client.get_object(bucket_name, object_name)
        except ResponseError as err:
            logging.info(f"error getting the object: {err}")
            raise err

    @contextlib.contextmanager
    def download_file(self, file: str, bucket_name: str) -> Iterator[Path]:
        """Download file to temporary local directory."""
        try:  # TODO: move this to decorator
            if not self.is_connected:
                self.connect()

            response = self.download_blob_response(file, bucket_name)
            gevent.sleep(0.001)
            with tempfile.TemporaryDirectory() as tmpdir:
                filepath = tmpdir / Path(f"{file}")

                with open(filepath, "wb") as file_data:
                    gevent.sleep(0.001)
                    for d in response.stream(64 * 1024):
                        gevent.sleep(0.001)
                        file_data.write(d)

                if filepath.exists():
                    gevent.sleep(0.001)
                    yield filepath
                else:
                    raise OSError("file streaming failed")

        except minio.ResponseError as err:
            logging.error(f"{err}")
            raise err

    def list_bucket(self, bucket_name: str, limit: int = 250) -> List[str]:
        """List files in a bucket."""
        objects = self.client.list_objects_v2(bucket_name)

        result = []
        for _ in range(limit):
            try:
                obj = next(objects)
                result.append(obj.object_name)
            except StopIteration:
                pass

        return result

    def rename(self, old_object_name: str, new_object_name: str, bucket_name: str):
        """Rename object."""
        tempdir = tempfile.TemporaryDirectory()
        local_file = Path(tempdir.name) / old_object_name

        self.client.fget_object(bucket_name, old_object_name, str(local_file))
        self.client.fput_object(bucket_name, new_object_name, str(local_file))

        self.client.remove_object(bucket_name, old_object_name)
        local_file.unlink()
