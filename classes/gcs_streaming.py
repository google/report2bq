"""
Copyright 2020 Google LLC

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

import logging

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common
from google.cloud import storage
from threading import Thread, Event
from queue import Queue, Empty

class GCSStreamingUploader(object):
  streamer_type = 'Undefined'

  def __enter__(self):
    self.start()
    return self


  def __exit__(self, exc_type, *_):
    if exc_type is None:
      self.stop()


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
    logging.info((
      '%s stopping... final write count: %s '
      'bytes in %d chunks'),
      self.streamer_type, f'{self._request.bytes_uploaded:,}', self._chunk_id )


  def write(self, data: bytes) -> int:
    data_len = len(data)
    self._buffer_size += data_len
    self._buffer += data
    del data
    while self._buffer_size >= self._chunk_size:
      try:
        self._request.transmit_next_chunk(self._transport)
        logging.info('%s written %s bytes',
          self.streamer_type, f'{self._request.bytes_uploaded:,}')
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


class GCSObjectStreamUpload(GCSStreamingUploader):

  def __init__(
    self,
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    chunk_size: int = 256 * 1024
  ):
    self._client = client
    self._bucket = self._client.bucket(bucket_name)
    self._blob = self._bucket.blob(blob_name)

    self._buffer = b''
    self._buffer_size = 0
    self._chunk_size = chunk_size
    self._read = 0

    self._transport = AuthorizedSession(
        credentials=self._client._credentials
    )
    self._request = None  # type: requests.ResumableUpload
    self.streamer_type = 'GCS Streamer'
    logging.info('%s initialized', self.streamer_type)


class ThreadedGCSObjectStreamUpload(GCSStreamingUploader, Thread):

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
    self.streamer_type = 'Threaded GCS Streamer'
    logging.info('%s initialized', self.streamer_type)


  def stopped(self):
    return self._stop.isSet()


  def run(self):
    logging.info('Threaded GCS Streamer starting')
    self.begin()

    while not self.stopped():
      # Get the work from the queue and expand the tuple
      try:
        (self._chunk_id, chunk) = self._queue.get(timeout=5)

      except Empty:
        continue

      try:
        logging.info('%s Grabbing chunk %d (%s bytes)',
          self.streamer_type, self._chunk_id, f'{len(chunk):,}')
        self.write(chunk)

      finally:
        self._queue.task_done()


  def stop(self):
    GCSStreamingUploader.stop(self)
    self._stop.set()
