"""
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

__author__ = [
  'davidharcombe@google.com (David Harcombe)'
]

import logging

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
from google.cloud import storage
from threading import Thread, Event
from queue import Queue, Empty


class ThreadedGCSObjectStreamUpload(Thread):

  def __init__(self, 
        client: storage.Client,
        bucket_name: str,
        blob_name: str,
        queue: Queue,
        chunk_size: int=256 * 1024,
    ):
    Thread.__init__(self)
    self._client = client
    self._bucket = self._client.bucket(bucket_name)
    self._blob = self._bucket.blob(blob_name)

    self._buffer = b''
    self._buffer_size = 0
    self._chunk_size = chunk_size
    self._read = 0
    self._bytes_written = 0
    self._chunk_id = 0
    self._queue = queue

    self._transport = AuthorizedSession(
        credentials=self._client._credentials
    )
    self._request = None  # type: requests.ResumableUpload

    self._stop = Event()
    logging.info('GCS Streamer initialized')


  def __enter__(self):
    self.start()
    return self


  def __exit__(self, exc_type, *_):
    if exc_type is None:
      self.stop()


  def stopped(self):
    return self._stop.isSet()


  def run(self):
    logging.info('GCS Streamer starting')
    self.begin()

    while not self.stopped():
      # Get the work from the queue and expand the tuple
      try:
        (self._chunk_id, chunk) = self._queue.get(timeout=5)

      except Empty:
        continue

      try:
        # logging.info(f'Grabbing chunk {self._chunk_id} ({len(chunk):,} bytes)')
        self.write(chunk)

      finally:
        self._queue.task_done()



  def begin(self):
    url = (
        f'https://www.googleapis.com/upload/storage/v1/b/'
        f'{self._bucket.name}/o?uploadType=resumable'
    )
    self._request = requests.ResumableUpload(
        upload_url=url, chunk_size=self._chunk_size
    )
    self._request.initiate(
        transport=self._transport,
        content_type='application/octet-stream',
        stream=self,
        stream_final=False,
        metadata={'name': self._blob.name},
    )


  def stop(self):
    self._request.transmit_next_chunk(self._transport)
    logging.info(f'GCS Streamer stopping... final write count: {self._request.bytes_uploaded:,} bytes in {self._chunk_id} chunks')
    self._stop.set()


  def write(self, data: bytes) -> int:
    data_len = len(data)
    self._buffer_size += data_len
    self._buffer += data
    del data
    while self._buffer_size >= self._chunk_size:
      try:
        self._request.transmit_next_chunk(self._transport)
        # logging.info(f'Written {self._request.bytes_uploaded:,} bytes')
        self._bytes_written += self._request.bytes_uploaded
      except common.InvalidResponse:
        self._request.recover(self._transport)
      
    return self._request.bytes_uploaded


  def read(self, chunk_size: int) -> bytes:
    to_read = min(chunk_size, self._buffer_size)
    memview = memoryview(self._buffer)
    self._buffer = memview[to_read:].tobytes()
    self._read += to_read
    self._buffer_size -= to_read
    return memview[:to_read].tobytes()


  def tell(self) -> int:
    return self._read
