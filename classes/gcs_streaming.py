# Lint as: python3
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Stream data to a file in Cloud Storage."""
from __future__ import annotations

import logging
import queue
import threading
from typing import Optional

from google.auth import credentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from google.resumable_media import common
from google.resumable_media import requests
# from gps_building_blocks.cloud.utils import cloud_auth


class GCSStreamingUploader(object):
  """Parent class with common code for single- and multi-threaded streamers."""
  streamer_type = 'Undefined'

  def __init__(self,
               bucket_name: str,
               blob_name: str,
               client: Optional[storage.Client] = None,
               creds: Optional[credentials.Credentials] = None,
               chunk_size: Optional[int] = 256 * 1024) -> GCSStreamingUploader:
    """Initialise a single-threaded streamer.

    Args:
        bucket_name (str): The name of the bucket to write to.
        blob_name (str): The name of the blob (file) to create.
        client (storage.Client): The GCS client. None will create a new client
          with the supplied credentials.
        creds (credentials.Credentials): The GCP credentials to use. None
          implies the default project credentials.
        chunk_size (int, optional): Size of buffer to write in a block. This
          defaults to 256*1024, or 256k (designed for restricted memory
          situations like a Cloud Function).
    """
    self._credentials = creds # if creds else cloud_auth.get_default_credentials()
    self._client = \
      client if client else storage.Client(credentials=self._credentials)
    self._client = storage.Client(credentials=self._credentials)
    self._bucket = self._client.get_bucket(bucket_name)
    self._blob = self._bucket.blob(blob_name)

    self._buffer = b''
    self._buffer_size = 0
    self._chunk_size = chunk_size
    self._read = 0

    self._bytes_written = 0
    self._request = None  # type: requests.ResumableUpload

  def begin(self):
    """Begin the streaming process.

    This method opens the resumable request and creates the destination blob
    ready for use.
    """
    self._transport = AuthorizedSession(credentials=self._credentials)
    url = (f'https://www.googleapis.com/upload/storage/v1/b/'
           f'{self._bucket.name}/o?uploadType=resumable')
    self._request = requests.ResumableUpload(
        upload_url=url, chunk_size=self._chunk_size)
    self._request.initiate(
        transport=self._transport,
        content_type='application/octet-stream',
        stream=self,
        stream_final=False,
        metadata={'name': self._blob.name},
    )

  def stop(self):
    """End the streaming process.

    Send the final chunk (regardless of size) to end the process.
    """
    self._request.transmit_next_chunk(self._transport)
    logging.info('%s stopping... final write count: %s bytes',
                 self.streamer_type, f'{self._request.bytes_uploaded:,}')

  def write(self, data: bytes) -> int:
    """Write the buffer content.

    Args:
        data (bytes): The data to be streamed to the GCS blob.

    Returns:
        int: number of bytes written
    """
    data_len = len(data)
    self._buffer_size += data_len
    self._buffer += data
    del data
    while self._buffer_size >= self._chunk_size:
      try:
        logging.info('%s writing chunk', self.streamer_type)
        self._request.transmit_next_chunk(
            transport=self._transport, timeout=180)
        logging.info('%s written %s bytes', self.streamer_type,
                     f'{self._request.bytes_uploaded:,}')
        self._bytes_written += self._request.bytes_uploaded
      except common.InvalidResponse:
        self._request.recover(self._transport)

    return self._request.bytes_uploaded

  def read(self, chunk_size: int) -> bytes:
    """Read bytes from the buffer.

    Args:
        chunk_size (int): number of bytes to read.

    Returns:
        bytes: The bytes read.
    """
    to_read = min(chunk_size, self._buffer_size)
    memview = memoryview(self._buffer)
    self._buffer = memview[to_read:].tobytes()
    self._read += to_read
    self._buffer_size -= to_read
    return memview[:to_read].tobytes()

  def tell(self) -> int:
    """Report the current position in the buffer.

    Returns:
        int: Position in the buffer.
    """
    return self._read


class GCSObjectStreamUpload(GCSStreamingUploader):
  """Single-threaded streamer.

  This is used where time is not of the essence, but memory is: for example, you
  can be needing to upload a large (let's say 10Gb) blob that is held remotely
  but you have no limit on how long the process can run. The
  `GCSObjectStreamUpload` will allow you to read a block, then immediately
  stream it to GCS, thus allowing you to process a file you could not store
  entirely in memory but have no local filesystem to download to first.

  Typical usage example:
      chunk_size = self.chunk_multiplier * 1024 * 1024
      streamer = ThreadedGCSObjectStreamUpload(
          client=CloudStorageUtils(project_id='project').client,
          bucket_name='bucket',
          blob_name='test.csv',
          chunk_size=chunk_size)
      streamer.begin()

      with open('source.txt') as source:
        chunk = source.read(chunk_size)
        streamer.write(chunk)

      streamer.stop()
  """

  def __init__(self,
               bucket_name: str,
               blob_name: str,
               client: Optional[storage.Client] = None,
               creds: Optional[credentials.Credentials] = None,
               chunk_size: Optional[int] = 256 * 1024):
    """Initialise a single-threaded streamer.

    Args:
        bucket_name (str): The name of the bucket to write to.
        blob_name (str): The name of the blob (file) to create.
        client (storage.Client): The GCS client. None will create a new client
          with the supplied credentials.
        creds (credentials.Credentials): The GCP credentials to use. None
          implies the default project credentials.
        chunk_size (int, optional): Size of buffer to write in a block. This
          defaults to 256*1024, or 256k (designed for restricted memory
          situations like a Cloud Function).
    """
    super().__init__(client=client,
                     creds=creds,
                     bucket_name=bucket_name,
                     blob_name=blob_name,
                     chunk_size=chunk_size)
    self.streamer_type = 'GCS Streamer'
    logging.info('%s initialized', self.streamer_type)


class ThreadedGCSObjectStreamUpload(GCSStreamingUploader, threading.Thread):
  """Multi-threaded streamer.

  This is used where both time and memory are restricted: for example, you
  can be needing to upload a large (let's say 10Gb) blob that is held remotely
  and (in a Cloud Function) your process runtime has a maximum.
  `ThreadedGCSObjectStreamUpload` will allow you to read a block from the
  source and drop it in the queue, immediately moving on to the next source
  block download while the threaded streamer begins the (usually slower)
  upload to GCS in the background.

  It also allows for processing of the source data post-download - for example
  cleaning of CSVs, removal of PII from a CSV/Text file etc - before the block
  is streamed out to GCS, and if the resultant block is smaller or larger than
  the buffer it doesn't matter; the streamer will write in units of the
  `chunk_size` and will wait until it has sufficient data OR it has pulled the
  last chunk from the queue (ie the queue is `Empty` and the `.join()`
  method has been reached) at which point it flushes the remaining data and
  stops.

  Typical usage example:
      streamer_queue = queue.Queue()
      chunk_size = self.chunk_multiplier * 1024 * 1024
      streamer = ThreadedGCSObjectStreamUpload(
          client=CloudStorageUtils(project_id='project').client,
          bucket_name='bucket',
          blob_name='test.csv',
          chunk_size=chunk_size,
          streamer_queue=streamer_queue)
      streamer.start()

      with open('source.txt') as source:
        chunk = source.read(chunk_size)
        queue.put(chunk)

      streamer_queue.join()
      streamer.stop()
  """

  def __init__(self,
               bucket_name: str,
               blob_name: str,
               streamer_queue: queue.Queue,
               client: Optional[storage.Client] = None,
               creds: Optional[credentials.Credentials] = None,
               chunk_size: Optional[int] = 256 * 1024):
    """Initialise a new ThreadedGCSStreamer.

    Args:
        bucket_name (str): The name of the bucket to write to.
        blob_name (str): The name of the blob (file) to create.
        streamer_queue (queue.Queue): The queue holding the bytes to write.
        client (storage.Client): The GCS client. None will create a new client
          with the supplied credentials.
        creds (credentials.Credentials): The GCP credentials to use. None
          implies the default project credentials.
        chunk_size (int, optional): Size of buffer to write in a block. This
          defaults to 256*1024, or 256k (designed for restricted memory
          situations like a Cloud Function).
    """
    super().__init__(client=client,
                     creds=creds,
                     bucket_name=bucket_name,
                     blob_name=blob_name,
                     chunk_size=chunk_size)
    threading.Thread.__init__(self)
    self._queue = streamer_queue
    self._stop = threading.Event()
    self.streamer_type = 'Threaded GCS Streamer'
    logging.info('%s initialized', self.streamer_type)

  def stopped(self):
    """Check if the thread is running.

    Returns:
        bool: Is the thread stopped.
    """
    return self._stop.is_set()

  def start(self):
    threading.Thread.start(self)

  def run(self) -> None:
    """Thread start method to run the streamer."""
    logging.info('Threaded GCS Streamer starting')
    self.begin()

    while not self.stopped():
      # Get the work from the queue and expand the tuple
      try:
        chunk = self._queue.get(timeout=5)

      except queue.Empty:
        continue

      try:
        logging.info('%s Grabbing chunk of %s bytes', self.streamer_type,
                     f'{len(chunk):,}')
        self.write(chunk)

      finally:
        self._queue.task_done()

  def stop(self):
    """Stop the streamer."""
    GCSStreamingUploader.stop(self)
    self._stop.set()
