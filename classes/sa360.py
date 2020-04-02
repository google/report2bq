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

# Python Imports
import csv
import json
import logging
import time

from bs4 import BeautifulSoup
from io import BytesIO, StringIO
from typing import Dict, List, Any
from xml.dom import minidom

from httplib2 import Http
from apiclient import discovery
from oauth2client.client import AccessTokenCredentials
from classes.credentials import Credentials
from classes.cloud_storage import Cloud_Storage
from classes.csv_helpers import CSVHelpers
from classes.threaded_streamer import ThreadedGCSObjectStreamUpload
from classes.decorators import timeit, measure_memory

from google.auth.transport.requests import AuthorizedSession
from google.resumable_media import requests, common

# Other imports
from queue import Queue, Empty


class SA360(object):
  def __init__(self, email: str, project: str, storage: Cloud_Storage):
    self.email = email
    self.project = project
    self.storage = storage
    self.credentials = storage.client._credentials
    self.transport = AuthorizedSession(credentials=self.credentials)
     

  def _soupify(self, data: BytesIO) -> BeautifulSoup:
    return BeautifulSoup(data, 'lxml')


  def process(self, bucket: str, report_details: Dict[str, Any]) -> None:
    input_buffer = BytesIO()
    self._fetch_data(report_details=report_details, buffer=input_buffer)
    # report_details['id'] = 'copied'
    # self.upload_report(bucket=bucket, report_details=report_details, input_buffer=input_buffer)
    # report_details['id'] = 'chunked'
    self.chunked_report(bucket=bucket, report_details=report_details, input_buffer=input_buffer)


  @timeit
  def _fetch_data(self, report_details: Dict[str, Any], buffer: BytesIO) -> int:
    try:
      report_url = report_details['url']

      request = requests.Download(report_url, stream=buffer)
      response = request.consume(transport=self.transport)
      return 0

    except Exception as e:
      logging.error(e)

    return -1


  @measure_memory
  def upload_report(self, bucket: str, report_details: Dict[str, Any], input_buffer: BytesIO=None):
    report_url = report_details['url']
    output_buffer = StringIO() #BytesIO()

    try:
      if not input_buffer:
        input_buffer = BytesIO()
        request = requests.Download(report_url, stream=input_buffer)
        response = request.consume(transport=self.transport)
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
        file='{id}.csv'.format(id=report_details['id']), 
        data=output_buffer.getvalue())
      report_details['schema'] = CSVHelpers.create_table_schema(fieldnames)

    except Exception as e:
      logging.error(e)


  @timeit
  def handle_report(self, bucket: str, report_details: Dict[str, Any]):
    report_url = report_details['url']
    input_buffer = BytesIO()
    remainder = b''
    prepender = b''
    queue = Queue()
    output_buffer = StringIO()
    chunk_size = 1024 * 1024
    streamer = ThreadedGCSObjectStreamUpload(client=self.storage.client,
                                             bucket_name=bucket,
                                             blob_name='{id}.csv'.format(id=report_details['id']),
                                             chunk_size=chunk_size,
                                             queue=queue)
    streamer.start()

    try:
      chunk_id = 0
      downloader = requests.ChunkedDownload(
        media_url=report_url,
        chunk_size=chunk_size,
        stream=input_buffer)

      while not downloader.finished:
        response = downloader.consume_next_chunk(transport=self.transport)

        # find last </tr> on any section but the last
        if not downloader.finished:
          if remainder:
            prepender = remainder
          else:
            prepender = b''

          # chop off the last portion and store
          last_tr_pos = input_buffer.getvalue().rfind(b'</tr>')
          if last_tr_pos != -1:
            input_buffer.seek(last_tr_pos)
            remainder = input_buffer.read(-1)
            input_buffer.truncate(last_tr_pos)

        # soupify
        input_buffer.seek(0)

        # if #0, get headers
        if chunk_id == 0:
          soup = self._soupify(input_buffer)
          headers = soup.find('thead').find_all('th')
          fieldnames = []
          for header in headers:
            fieldnames.append(CSVHelpers.sanitize_string(header.string))

          rows = soup.find('tbody').find_all('tr')

        else:
          b = BytesIO(initial_bytes=(prepender + input_buffer.getvalue()))
          soup = self._soupify(b)
          rows = soup.find_all('tr')
         

        # queue for upload
        report_data = []
        for row in rows:
          data = []
          for col in row.contents:
            data.append(col.string)
          report_data.append(dict(zip(fieldnames, data)))

        writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
        if chunk_id == 0: writer.writeheader()

        for row in report_data:
          writer.writerow(row)

        output_buffer.seek(0)
        queue.put((chunk_id, output_buffer.getvalue().encode('utf-8')))
        chunk_id += 1
        output_buffer.seek(0)
        output_buffer.truncate(0)

      queue.join()
      streamer.stop()
      report_details['schema'] = CSVHelpers.create_table_schema(fieldnames)

    except Exception as e:
      logging.error(e)


  @measure_memory
  def chunked_report(self, bucket: str, report_details: Dict[str, Any], input_buffer: BytesIO=None):
    report_url = report_details['url']
    remainder = b''
    prepender = b''
    queue = Queue()
    output_buffer = StringIO()
    html_chunk_size = 2048 * 1024
    chunk_size = 256 * 1024
    streamer = ThreadedGCSObjectStreamUpload(client=self.storage.client,
                                             bucket_name=bucket,
                                             blob_name='{id}.csv'.format(id=report_details['id']),
                                             chunk_size=chunk_size,
                                             queue=queue)

    try:
      chunk_id = 0
      if not input_buffer:
        input_buffer = BytesIO()
        request = requests.Download(report_url, stream=input_buffer)
        request.consume(transport=self.transport)

      input_buffer.seek(0)
      chunking = True
      streamer.start()

      while chunking:
        logging.info('Souping chunk {chunk}'.format(chunk=chunk_id))
        chunk = BytesIO()
        chunk.write(remainder)
        next_chunk = input_buffer.read(html_chunk_size)
        if not next_chunk:
          chunking = False
        else:
          chunk.write(next_chunk)

        chunk.seek(0)

        # find last </tr> on any section but the last
        # chop off the last portion and store
        last_tr_pos = chunk.getvalue().rfind(b'</tr>')
        if last_tr_pos != -1:
          last_tr_pos += 5
          chunk.seek(last_tr_pos)
          remainder = chunk.read()
          chunk.truncate(last_tr_pos)

        # soupify
        chunk.seek(0)

        # if #0, get headers
        if chunk_id == 0:
          soup = self._soupify(chunk)
          headers = soup.find('thead').find_all('th')
          fieldnames = []
          for header in headers:
            fieldnames.append(CSVHelpers.sanitize_string(header.string))

          rows = soup.find('tbody').find_all('tr')

        else:
          b = BytesIO(initial_bytes=(prepender + chunk.getvalue()))
          soup = self._soupify(b)
          rows = soup.find_all('tr')
         

        # queue for upload
        report_data = []
        for row in rows:
          data = []
          for col in row.contents:
            data.append(col.string)
          report_data.append(dict(zip(fieldnames, data)))

        writer = csv.DictWriter(output_buffer, fieldnames=fieldnames)
        if chunk_id == 0: writer.writeheader()

        for row in report_data:
          writer.writerow(row)

        output_buffer.seek(0)
        queue.put((chunk_id, output_buffer.getvalue().encode('utf-8')))
        chunk_id += 1
        output_buffer.seek(0)
        output_buffer.truncate(0)

      queue.join()
      streamer.stop()
      report_details['schema'] = CSVHelpers.create_table_schema(fieldnames)

    except Exception as e:
      logging.error(e)