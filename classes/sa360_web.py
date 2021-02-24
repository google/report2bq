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

__author__ = ['davidharcombe@google.com (David Harcombe)']

# Python Imports
from classes import ReportFetcher
import csv
import logging
import os
import re
import requests as req

from html.parser import unescape
from io import BytesIO, StringIO, SEEK_END
from typing import Dict, List, Any, Tuple
from urllib.parse import unquote

from classes.credentials import Credentials
from classes.cloud_storage import Cloud_Storage
from classes.csv_helpers import CSVHelpers
from classes.decorators import timeit, measure_memory
from classes.firestore import Firestore
from classes.report_type import Type
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload

from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage

# Other imports
from contextlib import closing
from queue import Queue
from urllib.request import urlopen


class SA360Web(ReportFetcher):
  report_type = Type.SA360
  email = None
  project = None
  profile = None

  def __init__(self,
               email: str,
               project: str,
               append: bool = False,
               infer_schema: bool = False):
    self.email = email
    self.project = project
    self.creds = Credentials(email=email, project=project)
    self.credentials = storage.Client()._credentials
    self.transport = AuthorizedSession(credentials=self.credentials)
    self.append = append
    self.infer_schema = infer_schema

    self.firestore = Firestore(email=email, project=project)

    self.chunk_multiplier = int(os.environ.get('CHUNK_MULTIPLIER', 64))
    self.bucket = f'{self.project}-report2bq-upload'

  @measure_memory
  def stream_to_gcs(self, bucket: str, report_details: Dict[str, Any]) -> None:
    report_url = report_details['url']
    remainder = b''
    queue = Queue()
    output_buffer = StringIO()

    # size of pieces of html we can safely and easily download from the web report.
    html_chunk_size = 2048 * 1024

    # chunk_multiplier is set in the environment, but defaults to 64 - this leads to a
    # 64M chunk size we can throw around. Given the memory constraints of a cloud function
    # this seems like a good, safe number.
    chunk_size = self.chunk_multiplier * 1024 * 1024
    streamer = ThreadedGCSObjectStreamUpload(
      client=Cloud_Storage.client(credentials=self.creds),
      bucket_name=bucket,
      blob_name='{id}.csv'.format(id=report_details['id']),
      chunk_size=chunk_size,
      streamer_queue=queue)
    streamer.daemon = True
    streamer.start()

    try:
      chunk_id = 0
      conn = self.get_connection(report_url)
      _stream = conn.iter_content(chunk_size=html_chunk_size)
      source_size = 0

      done = False
      fieldnames = None

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

        if chunk_id == 0:
          fieldnames, chunk = self.find_fieldnames(buffer=chunk)

        # find last </tr> on any section but the last, chop off the last portion and store
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
        if chunk_id == 0:
          writer.writeheader()

        [writer.writerow(row) for row in report_data]

        output_buffer.seek(0)
        queue.put(output_buffer.getvalue().encode('utf-8'))
        chunk_id += 1
        chunk = BytesIO()
        output_buffer.seek(0)
        output_buffer.truncate(0)

      logging.info(f'SA360 report length: {source_size:,} bytes')
      queue.join()
      streamer.stop()
      report_details['schema'] = CSVHelpers.create_table_schema(fieldnames)

    except Exception as e:
      logging.error(e)

  def next_chunk(self,
                 stream,
                 html_chunk_size: int = None) -> Tuple[bytes, bool]:
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
    header, buffer = self.extract_keys(buffer=buffer, key='thead')
    if header:
      fieldnames = [
        CSVHelpers.sanitize_string(field)
        for field in re.findall(r'\<th[^>]*\>([^<]*)\<\/th\>', header)
      ]
      # logging.info(f'Fields: {fieldnames}')
      del header
    else:
      fieldnames = None

    return fieldnames, buffer

  @timeit
  def get_connection(self, report_url: str):
    auth_headers = self.creds.get_auth_headers()
    conn = req.get(report_url, stream=True, headers=auth_headers)
    return conn
