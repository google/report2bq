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
import logging
import urllib.request
import os

from io import BytesIO
from typing import Dict, List, Any, Tuple
from classes import secret_manager_credentials as credentials

from classes.secret_manager_credentials import Credentials
from classes.cloud_storage import Cloud_Storage
from classes import csv_helpers
from classes.decorators import measure_memory
from classes import discovery
from classes.firestore import Firestore
from classes.report_type import Type
from classes.services import Service
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload

from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
from googleapiclient.discovery import Resource

# Other imports
from contextlib import closing
from queue import Queue
from urllib.request import urlopen


class SA360Dynamic(ReportFetcher):
  report_type = Type.SA360_RPT
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

  def service(self) -> Resource:
    return discovery.get_service(service=Service.SA360, credentials=self.creds)

  def handle_report(self, run_config: Dict[str, Any]) -> bool:
    sa360_service = self.service()
    request = sa360_service.reports().get(reportId=run_config['file_id'])

    try:
      report = request.execute()

      if report['isReportReady']:
        report_config = self.firestore.get_document(
          type=Type.SA360_RPT, id=run_config['report_id'])

        csv_header, _ = self.read_header(report)
        schema = csv_helpers.create_table_schema(csv_header, None)
        report_config['schema'] = schema
        report_config['files'] = report['files']

        if 'dest_project' in run_config:
          report_config['dest_project'] = run_config['dest_project']
        if 'dest_dataset' in run_config:
          report_config['dest_dataset'] = run_config['dest_dataset']
        if 'notify_message' in run_config:
          report_config['notifier']['message'] = run_config['notify_message']

        # update the report details please...
        self.firestore.update_document(Type.SA360_RPT, run_config['report_id'],
                                       report_config)

        # ... then stream the file to GCS a la DV360/CM
        self.stream_to_gcs(report_details=report_config, run_config=run_config)

      return report['isReportReady']

    except Exception as e:
      logging.error(
        f'Report fetch error: Run {run_config["file_id"]} for report {run_config["report_id"]}'
      )
      return False

  def read_header(self, report_config: dict) -> list:
    r = urllib.request.Request(report_config['files'][0]['url'])
    for header in self.creds.auth_headers:
      r.add_header(header, self.creds.auth_headers[header])

    with closing(urlopen(r)) as report:
      data = report.read(self.chunk_multiplier * 1024 * 1024)
      bytes_io = BytesIO(data)

    return csv_helpers.get_column_types(bytes_io)

  @measure_memory
  def stream_to_gcs(self, report_details: Dict[str, Any],
                    run_config: Dict[str, Any]) -> None:
    """Multi-threaded stream to GCS

    Arguments:
        bucket (str):  GCS Bucket
        report_details (dict):  Report definition
    """
    queue = Queue()

    report_id = run_config['report_id']

    # chunk_multiplier is set in the environment, but defaults to 64 - this leads to a
    # 64M chunk size we can throw around. Given the memory constraints of a cloud function
    # this seems like a good, safe number.
    chunk_size = self.chunk_multiplier * 1024 * 1024
    out_file = BytesIO()

    streamer = \
      ThreadedGCSObjectStreamUpload(
        client=Cloud_Storage.client(),
        creds=credentials.Credentials(
          email=self.email, project=self.project).credentials,
        bucket_name=self.bucket,
        blob_name=f'{report_id}.csv',
        chunk_size=chunk_size,
        streamer_queue=queue)
    streamer.start()

    r = urllib.request.Request(report_details['files'][0]['url'])
    for header in self.creds.auth_headers:
      r.add_header(header, self.creds.auth_headers[header])

    with closing(urlopen(r)) as _report:
      _downloaded = 0
      chunk_id = 1
      _report_size = int(_report.headers['content-length'])
      while _downloaded < _report_size:
        chunk = _report.read(chunk_size)
        _downloaded += len(chunk)
        queue.put(chunk)
        chunk_id += 1

    queue.join()
    streamer.stop()
