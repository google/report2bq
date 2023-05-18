# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import csv
import logging
import os
import re
from html.parser import unescape
from io import BytesIO, StringIO
from queue import Queue
from typing import Any, Generator, List, Tuple

import requests as req
from auth.credentials import Credentials
from auth.datastore.secret_manager import SecretManager
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage

from classes import ReportFetcher, csv_helpers
from classes.cloud_storage import Cloud_Storage
from classes.decorators import retry, timeit
from classes.firestore import Firestore
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload
from classes.report_config import ReportConfig
from classes.report_type import Type


class SA360Exception(Exception):
  """SA360Exception.

  A custom SA360 exception for the process that we can use to optionally
  retry.
  """
  pass


class SA360Web(ReportFetcher):
  """SA360Web Downloadable Processor.

  This class process the Microsoft XML-format web downloadable (webqueryphtml)
  reports and converts them to CSV, storing them in GCS for the Report2BQ
  loader.
  """
  report_type = Type.SA360
  email = None
  project = None
  profile = None

  def __init__(self,
               email: str,
               project: str,
               append: bool = False,
               infer_schema: bool = False) -> SA360Web:
    self.email = email
    self.project = project
    self.creds = Credentials(datastore=SecretManager,
                             email=email, project=project)
    self.credentials = storage.Client()._credentials
    self.transport = AuthorizedSession(credentials=self.credentials)
    self.append = append
    self.infer_schema = infer_schema

    self.firestore = Firestore(email=email, project=project)

    # chunk_multiplier is set in the environment, but defaults to 64 - this
    # leads to a 64M chunk size we can throw around. Given the memory
    # constraints of a cloud function this seems like a good, safe number.
    self.chunk_multiplier = int(os.environ.get('CHUNK_MULTIPLIER', 64))
    self.bucket = f'{self.project}-report2bq-upload'

  @retry(SA360Exception, tries=2)
  def stream_to_gcs(self, bucket: str, report_details: ReportConfig) \
          -> Tuple[List[str], List[str]]:
    """Streams the data to Google Cloud Storage.

    This is to allow us to process much larger files than can be easily
    handled in toto in memory. Now we're limited to length of execution (900s)
    rather than size of 'stuff' (<2Gb).

    The response from SA360 is a _nasty_ piece of Microsoft Office format XML
    which has to be parsed and converted to a digestible CSV.

    Raises:
        SA360Exception: A custom SA360 exception because there can be a server
                        error returned when requesting, but the error is in
                        text and the HTTP code returned is _always_ a 200.
                        This is why the function is allowed to retry, as the
                        error is usually transient and caused by a failure to
                        connect to SA360's reporting back end.

    Returns:
        (fieldnames: List[str], fieldtypes: List[str]):
          the field names and types in the report.
    """
    report_url = report_details.url
    remainder = b''
    queue = Queue()
    output_buffer = StringIO()

    # size of pieces of xml we can safely download from the web report.
    html_chunk_size = 2048 * 1024
    chunk_size = self.chunk_multiplier * 1024 * 1024

    streamer = ThreadedGCSObjectStreamUpload(
        client=Cloud_Storage.client(credentials=self.creds),
        creds=self.creds.credentials,
        bucket_name=bucket,
        blob_name=f'{report_details.id}.csv',
        chunk_size=chunk_size,
        streamer_queue=queue)
    streamer.daemon = True
    streamer.start()

    chunk_id = 0
    conn = self.get_connection(report_url)
    _stream = conn.iter_content(chunk_size=html_chunk_size)
    source_size = 0

    first = True
    done = False
    fieldnames = None
    fieldtypes = None

    while not done:
      chunk = BytesIO()
      chunk.write(remainder)
      remainder = b''

      block, done = self.next_chunk(_stream, html_chunk_size)
      source_size += len(block)
      chunk.write(block)
      if len(chunk.getvalue()) < html_chunk_size and not done:
        continue

      chunk.seek(0)

      if first:
        fieldnames, chunk = self.find_fieldnames(buffer=chunk)
        if len(fieldnames) == 1 and fieldnames[0] == 'Error':
          error = \
              unescape(re.sub(r'<[^.]+>', '', chunk.getvalue().decode('utf-8')))
          # logging.error('SA360 Error: %s', error)
          streamer.stop()
          raise SA360Exception(error)

      # find last </tr> on any section but the last, chop off the last
      # portion and store
      last_tr_pos = chunk.getvalue().rfind(b'</tr>')
      if last_tr_pos == -1:
        remainder = chunk.getvalue()
        continue

      else:
        last_tr_pos += 5
        chunk.seek(last_tr_pos)
        remainder = chunk.read()
        chunk.truncate(last_tr_pos)

      rows = []
      while True:
        tr, chunk = self.extract_keys(chunk, 'tr')
        if chunk:
          rows.append([
              unescape(field)
              for field in re.findall(r'\<td[^>]*\>([^<]*)\<\/td\>', tr)
          ])
        else:
          break

      # queue for upload
      report_data = []
      for row in rows:
        report_data.append(dict(zip(fieldnames, row)))

      writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
      if first:
        writer.writeheader()

      [writer.writerow(row) for row in report_data]

      output_buffer.seek(0)

      if first:
        _, fieldtypes = \
            csv_helpers.get_column_types(
                BytesIO(output_buffer.getvalue().encode('utf-8')))

      queue.put(output_buffer.getvalue().encode('utf-8'))
      chunk_id += 1
      first = False
      chunk = BytesIO()
      output_buffer.seek(0)
      output_buffer.truncate(0)

    logging.info(f'SA360 report length: {source_size:,} bytes')
    queue.join()
    streamer.stop()
    report_details.schema = \
        csv_helpers.create_table_schema(fieldnames, fieldtypes)

    return fieldnames, fieldtypes

  def next_chunk(self,
                 stream: Generator[Any | bytes | str, None, None],
                 html_chunk_size: int = None) -> Tuple[bytes, bool]:
    """Fetches the next block of data.

    This grabs the next block of data from the HTTP stream.

    Args:
        stream (Generator[Any | bytes | str, None, None]): the response stream.
        html_chunk_size (int, optional): size of chunk to request.

    Returns:
        Tuple[bytes, bool]: bytes of data in theis chunk, marker to indicate
                            if this is the final chunk.
    """
    _buffer = BytesIO()
    last_chunk = False
    while len(_buffer.getvalue()) < html_chunk_size and not last_chunk:
      try:
        _block = stream.__next__()
        if _block:
          _buffer.write(_block)
      except StopIteration:
        last_chunk = True

    return _buffer.getvalue(), last_chunk

  def extract_keys(self, buffer: BytesIO, key: str) -> Tuple[str, BytesIO]:
    """Finds HTML keys and their content.

    Search the supplied buffer looking for the outermost set of matching HTML
    keys and return them as a stream and content string.

    Args:
        buffer (BytesIO): the buffer of data
        key (str): the html tag to extract

    Returns:
        Tuple[str, BytesIO]: resultant information
    """
    b = buffer.getvalue()
    start_pos = b.find((f'<{key}>').encode('utf-8'))
    if start_pos == -1:
      buffer.seek(0)
      extract = None
      new_stream = None
    else:
      end_pos = b.find((f'</{key}>').encode('utf-8'), start_pos)
      buffer.seek(start_pos)
      content = buffer.read(end_pos + len(f'</{key}>') - start_pos)
      extract = content.decode('utf-8')
      new_stream = BytesIO(buffer.read())

    return extract, new_stream

  def find_fieldnames(self, buffer: BytesIO) -> Tuple[str, BytesIO]:
    """Finds the field names in the report.

    Searched the stream for the XML header, and grabs all the listed columns in
    there.

    Args:
        buffer (BytesIO): the xml data.

    Returns:
        Tuple[str, BytesIO]: (fieldnames, remaining unprocessed buffer).
    """
    header, buffer = self.extract_keys(buffer=buffer, key='thead')
    if header:
      fieldnames = [
          csv_helpers.sanitize_column(field)
          for field in re.findall(r'\<th[^>]*\>([^<]*)\<\/th\>', header)
      ]
      del header
    else:
      fieldnames = None

    return fieldnames, buffer

  @timeit
  def get_connection(self, report_url: str) -> req.Response:
    """Opens a connection to the SA360 report.

    Args:
        report_url (str): the web URL, as a 'GET' style encoded URL.

    Returns:
        requests.Response: the response object (connection).
    """
    auth_headers = self.creds.auth_headers
    conn = req.get(report_url, stream=True, headers=auth_headers)
    return conn
