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

__author__ = [
  'davidharcombe@google.com (David Harcombe)'
]

# Python Imports
import csv
import json
import logging
import os
import re
import requests as req
import time
import urllib.request

from apiclient import discovery
from bs4 import BeautifulSoup
from html.parser import unescape
from httplib2 import Http
from io import BytesIO, StringIO, SEEK_END
from oauth2client.client import AccessTokenCredentials
from typing import Dict, List, Any, Tuple
from urllib.parse import unquote
from xml.dom import minidom

from classes.credentials import Credentials
from classes.cloud_storage import Cloud_Storage
from classes.csv_helpers import CSVHelpers
from classes.decorators import timeit, measure_memory
from classes.discovery import DiscoverService
from classes.firestore import Firestore
from classes.report_type import Type
from classes.services import Service
from classes.threaded_streamer import ThreadedGCSObjectStreamUpload

from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from google.resumable_media import requests, common

# Other imports
from contextlib import closing
from queue import Queue, Empty
from urllib.request import urlopen

class SA360(object):
  def __init__(self, email: str, project: str, append: bool=False, infer_schema: bool=False):
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


  def _soupify(self, data: BytesIO) -> BeautifulSoup:
    return BeautifulSoup(data, 'lxml')


  def process(self, bucket: str, report_details: Dict[str, Any]) -> None:
    input_buffer = BytesIO()
    repeater = self._stream_processor(bucket=bucket, report_details=report_details, repeatable=False)
    # old_id, report_details['id'] = report_details['id'], f'{report_details["id"]}-repeat'
    # self.upload_report(bucket=bucket, report_details=report_details, input_buffer=repeater)
    # report_details['id'] = old_id


  def handle_offline_report(self, run_config: Dict[str, Any]) -> bool:
    sa360_service = DiscoverService.get_service(Service.SA360, self.creds)
    request = sa360_service.reports().get(reportId=run_config['file_id'])

    try:
      report = request.execute()

      if report['isReportReady']:
        report_config = self.firestore.get_report_config(type=Type.SA360_RPT, id=run_config['report_id'])

        csv_header, _ = self.read_header(report)
        schema = CSVHelpers.create_table_schema(
          csv_header, 
          None
        )
        report_config['schema'] = schema
        report_config['files'] = report['files']

        if 'dest_project' in run_config: report_config['dest_project'] = run_config['dest_project']
        if 'dest_dataset' in run_config: report_config['dest_dataset'] = run_config['dest_dataset']
        if 'notify_topic' in run_config:
          report_config['notifier'] = {
            'topic': run_config['notify_topic'],
          }
          if 'notify_message' in run_config: report_config['notifier']['message'] = run_config['notify_message']

        # update the report details please...
        self.firestore.update_document(Type.SA360_RPT, run_config['report_id'], report_config)

        # ... then stream the file to GCS a la DV360/CM
        self._stream_report_to_gcs(report_details=report_config, run_config=run_config)

      return report['isReportReady']

    except Exception as e:
      logging.error(f'Report fetch error: Run {run_config["file_id"]} for report {run_config["report_id"]}')
      return False


  def read_header(self, report_config: dict) -> list:
    r = urllib.request.Request(report_config['files'][0]['url'])
    for header in self.creds.get_auth_headers():
      r.add_header(header, self.creds.get_auth_headers()[header])

    with closing(urlopen(r)) as report:
      data = report.read(self.chunk_multiplier * 1024 * 1024)
      bytes_io = BytesIO(data)

    return CSVHelpers.get_column_types(bytes_io)


  @measure_memory
  def _stream_report_to_gcs(self, report_details: Dict[str, Any], run_config: Dict[str, Any]) -> None:
    """Multi-threaded stream to GCS
    
    Arguments:
        bucket {str} -- GCS Bucket
        report_details {dict} -- Report definition
    """
    queue = Queue()

    report_id = run_config['report_id']
    chunk_size = self.chunk_multiplier * 1024 * 1024
    out_file = BytesIO()

    streamer = ThreadedGCSObjectStreamUpload(client=Cloud_Storage.client(), 
                                             bucket_name=self.bucket,
                                             blob_name=f'{report_id}.csv',
                                             chunk_size=chunk_size, 
                                             queue=queue)
    streamer.start()

    r = urllib.request.Request(report_details['files'][0]['url'])
    for header in self.creds.get_auth_headers():
      r.add_header(header, self.creds.get_auth_headers()[header])

    with closing(urlopen(r)) as _report:
      _downloaded = 0
      chunk_id = 1
      _report_size = int(_report.headers['content-length'])
      while _downloaded < _report_size:
        chunk = _report.read(chunk_size)
        _downloaded += len(chunk)
        queue.put((chunk_id, chunk))
        chunk_id += 1

    queue.join()
    streamer.stop()

  @timeit
  def _fetch_data(self, report_details: Dict[str, Any], buffer: BytesIO) -> int:
    try:
      report_url = report_details['url']

      request = requests.Download(report_url, stream=buffer)
      request.consume(transport=self.transport)
      return self._stream_size(buffer)

    except Exception as e:
      logging.error(e)

    return -1


  @timeit
  def _stream_size(self, buffer: BytesIO) -> int:
    pos = buffer.tell()
    buffer.seek(0, SEEK_END)
    size = buffer.tell()
    buffer.seek(pos)
    return size


  def _extract_keys(self, buffer: BytesIO, key: str) -> Tuple[str, BytesIO]:
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


  @timeit
  def _get_connection(self, report_url: str):
    auth_headers = self.creds.get_auth_headers()
    conn = req.get(report_url, stream=True, headers=auth_headers)
    return conn


  def _find_fieldnames(self, buffer: BytesIO) -> Tuple[str, BytesIO]:
    header, buffer = self._extract_keys(buffer=buffer, key='thead')
    if header:
      fieldnames = [CSVHelpers.sanitize_string(field) for field in re.findall(r'\<th[^>]*\>([^<]*)\<\/th\>', header)]
      # logging.info(f'Fields: {fieldnames}')
      del header
    else:
      fieldnames = None

    return fieldnames, buffer


  def _next_chunk(self, stream, html_chunk_size: int=None) -> Tuple[bytes, bool]:
    _buffer = BytesIO()
    last_chunk = False
    while len(_buffer.getvalue()) < html_chunk_size and not last_chunk:
      try:
        _block = stream.__next__()
        if _block: _buffer.write(_block)
      except StopIteration:
        last_chunk = True

    return _buffer.getvalue(), last_chunk


  @measure_memory
  def _stream_processor(self, bucket: str, report_details: Dict[str, Any], repeatable: bool=False) -> BytesIO:
    repeater = BytesIO()
    report_url = report_details['url']
    remainder = b''
    queue = Queue()
    output_buffer = StringIO()
    html_chunk_size = 2048 * 1024
    chunk_size = 1024 * 1024
    streamer = ThreadedGCSObjectStreamUpload(client=Cloud_Storage.client(credentials=self.creds),
                                             bucket_name=bucket,
                                             blob_name='{id}.csv'.format(id=report_details['id']),
                                             chunk_size=chunk_size,
                                             queue=queue)
    streamer.daemon = True
    streamer.start()

    try:
      chunk_id = 0
      conn = self._get_connection(report_url)
      _stream = conn.iter_content(chunk_size=html_chunk_size)
      source_size = 0

      done = False
      fieldnames = None

      while not done:
        # logging.info(f'Processing chunk {chunk_id}')
        # logging.info(f'Processing chunk {chunk_id}, remainder {remainder.decode("utf-8")}')
        chunk = BytesIO()
        chunk.write(remainder)
        remainder = b''

        block, done = self._next_chunk(_stream, html_chunk_size)
        source_size += len(block)
        # logging.info(f'{len(block):,}, begins {block[0:80]} : ends {block[-80:].decode("utf-8")}')
        if repeatable: repeater.write(block)
        chunk.write(block)
        if len(chunk.getvalue()) < html_chunk_size and not done:
          continue

        # logging.info(f'Chunk size {len(chunk.getvalue()):,} bytes')
        chunk.seek(0)

        if chunk_id == 0:
          fieldnames, chunk = self._find_fieldnames(buffer=chunk)

        # find last </tr> on any section but the last, chop off the last portion and store
        last_tr_pos = chunk.getvalue().rfind(b'</tr>')
        if last_tr_pos == -1:
          # logging.debug(f'HALP! {chunk.getvalue()}')
          remainder = chunk.getvalue()
          continue

        else:
          last_tr_pos += 5
          chunk.seek(last_tr_pos)
          remainder = chunk.read()
          # logging.debug(f'Remainder: {remainder}')
          chunk.truncate(last_tr_pos)

        rows = []
        while True:
          tr, chunk = self._extract_keys(chunk, 'tr')
          if chunk:
            rows.append([unescape(field) for field in re.findall(r'\<td[^>]*\>([^<]*)\<\/td\>', tr)])
          else:
            break

        # queue for upload
        report_data = []
        for row in rows:
          report_data.append(dict(zip(fieldnames, row)))

        writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
        if chunk_id == 0: writer.writeheader()

        [writer.writerow(row) for row in report_data]
         
        output_buffer.seek(0)
        # logging.info(f'Sending chunk {chunk_id} size {len(output_buffer.getvalue())}')
        queue.put((chunk_id, output_buffer.getvalue().encode('utf-8')))
        chunk_id += 1
        chunk = BytesIO()
        output_buffer.seek(0)
        output_buffer.truncate(0)

      logging.info(f'SA360 report length: {source_size:,} bytes')
      queue.join()
      streamer.stop()
      report_details['schema'] = CSVHelpers.create_table_schema(fieldnames)
      return repeater

    except Exception as e:
      logging.error(e)


  @measure_memory
  def upload_report(self, bucket: str, report_details: Dict[str, Any], input_buffer: BytesIO=None):
    output_buffer = StringIO() #BytesIO()

    try:
      if not input_buffer:
        input_buffer = BytesIO()
        request = requests.Download(report_details['url'], stream=input_buffer)
        request.consume(transport=self.transport)
        logging.info('Report data size: {bytes}'.format(bytes=0))

      input_buffer.seek(0)
      soup = self._soupify(input_buffer)
      # del input_buffer

      headers = soup.find('thead').find_all('th')
      fieldnames = []
      for header in headers:
        fieldnames.append(CSVHelpers.sanitize_string(header.string))

      rows = soup.find('tbody').find_all('tr')
      report_data = []
      for row in rows:
        data = []
        for col in row.contents:
          data.append(col.string)
        report_data.append(dict(zip(fieldnames, data)))

      writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
      writer.writeheader()

      for row in report_data:
        writer.writerow(row)

      output_buffer.seek(0)
      Cloud_Storage.write_file(
        bucket=bucket,
        file=f"{report_details['id']}.csv", 
        data=output_buffer.getvalue())
      report_details['schema'] = CSVHelpers.create_table_schema(fieldnames)

    except Exception as e:
      logging.error(e)

