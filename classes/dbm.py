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

import datetime
import io
import logging
import os
import re

from classes import Fetcher, ReportFetcher
from classes import credentials
from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials
from classes import csv_helpers
from classes.decorators import measure_memory, retry
from classes import discovery
from classes.report_type import Type
from classes.services import Service
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload

from contextlib import closing
from queue import Queue
from typing import Dict, Any, List, Tuple
from urllib.request import urlopen

from googleapiclient.discovery import Resource


class DBM(ReportFetcher, Fetcher):
  report_type = Type.DV360
  email = None
  project = None
  profile = None

  def __init__(self, email: str, project: str, profile: str=None) -> DBM:
    """
    Initialize Reporting Class
    """
    self.email = email
    self.project = project
    self.chunk_multiplier = int(os.environ.get('CHUNK_MULTIPLIER', 64))

  def service(self) -> Resource:
    return discovery.get_service(service=Service.DV360,
                                 credentials=Credentials(email=self.email,
                                                         project=self.project))

  def get_reports(self) -> List[Dict[str, Any]]:
    """
    Fetches list of reports for current user
    Returns:
      Report list
    """

    # Use Discovery Service to make api call
    # https://developers.google.com/apis-explorer/#p/doubleclickbidmanager/v1.1/doubleclickbidmanager.queries.listqueries
    result = self.fetch(
      self.service().queries().listqueries,
      **{'fields': 'queries(metadata(googleCloudStoragePathForLatestReport,latestReportRunTimeMs,title),params/type,queryId,schedule/frequency)'}
    )

    return result


  def get_report(self, report_id: str) -> Dict[str, Any]:
    """
    Fetches list of reports for current user
    Returns:
      Report list
    """

    # Use Discovery Service to make api call
    # https://developers.google.com/apis-explorer/#p/doubleclickbidmanager/v1.1/doubleclickbidmanager.queries.listqueries
    result = self.fetch(
      self.service().queries().getquery,
      **{
        'fields': 'metadata(googleCloudStoragePathForLatestReport,latestReportRunTimeMs,title),params/type,queryId,schedule/frequency',
        'queryId': report_id
      }
    )

    return result


  def get_latest_report_file(self, report_id: str):
    """
    Pulls a report objects from api report list return
    Args:
      reports: Reports object returned from queries api call
      report_id: report id
    Returns:
      Report object
    """
    report = {}
    all_results = self.fetch(
      self.service().reports().listreports,
      **{ 'queryId': report_id }
    )
    if all_results:
      if 'reports' in all_results:
        # filter out any still running reports or ones with no 'finishTimeMs'
        results = list(filter(
          lambda item: 'finishTimeMs' in item.get(
            'metadata', {}).get('status', {}),
          all_results['reports']))
        ordered = sorted(
          results,
          key=lambda k: int(k['metadata']['status']['finishTimeMs']))
        report = ordered[-1]
      else:
        logging.info('No reports - has this report run successfully yet?')

    return report


  def normalize_report_details(
    self, report_object: Dict[str, Any], report_id: str):
    """
    Normalize api results into flattened data structure
    Args:
      report_object: Report details from api queries method
    Returns:
      Normalized data structure
    """
    # Fetch query details too, as this contains the main piece
    query_object = \
      self.service().queries().getquery(queryId=report_id).execute()

    # Check if report has ever completed a run
    if(
        'latestReportRunTimeMs' in query_object['metadata']
        and query_object['metadata']['googleCloudStoragePathForLatestReport'] != ''
    ):
      # Exists
      gcs_path = query_object['metadata']['googleCloudStoragePathForLatestReport']
      latest_runtime = query_object['metadata']['latestReportRunTimeMs']

    else:
      # Report not yet run
      gcs_path = ''
      latest_runtime = 0

    # Normalize report data object
    report_data = {
      'id': query_object['queryId'],
      'name': query_object['metadata']['title'],
      'report_name':
          csv_helpers.sanitize_string(query_object['metadata']['title']),
      'type': query_object['params']['type'],
      'current_path': gcs_path,
      'last_updated': datetime.datetime.fromtimestamp(
          float(latest_runtime)/1000.0
      ).strftime("%Y%m%d%H%M"),
      'update_cadence': query_object['schedule']['frequency'],
    }

    # Return
    return report_data


  def fetch_report_config(self, report_object: Dict[str, Any], report_id: str):
    report_data = self.normalize_report_details(report_object=report_object, report_id=report_id)
    keys_to_update = [
      'email',
      'dest_dataset',
      'dest_project',
      'notifier',
      'schema',
      'append',
      'force',
      'infer_schema'
    ]

    for key in keys_to_update:
      if key in report_object:
        report_data[key] = report_object[key]

    return report_data


  @retry(Exception, tries=3, delay=15, backoff=2)
  def run_report(self, report_id: int, retry: int=0) -> Dict[str, Any]:
    result = {}
    request = self.service().queries().runquery(queryId=report_id)
    result = request.execute()

    return result


  @retry(Exception, tries=3, delay=15, backoff=2)
  def report_state(self, report_id: int):
    request = self.service().reports().listreports(queryId=report_id)
    results = request.execute()

    if results:
      ordered = sorted(results['reports'], key=lambda k: int(k['metadata']['reportDataStartTimeMs']))
      return ordered[-1]['metadata']['status']['state']

    else:
      return 'UNKNOWN'


  def read_header(self, report_details: dict) -> Tuple[List[str], List[str]]:
    if not 'current_path' in report_details:
      return (None, None)

    with closing(urlopen(report_details['current_path'])) as report:
      data = report.read(self.chunk_multiplier * 1024 * 1024)
      bytes_io = io.BytesIO(data)

    return csv_helpers.get_column_types(bytes_io)


  @measure_memory
  def stream_to_gcs(self, bucket: str, report_details: Dict[str, Any]) -> None:
    """Multi-threaded stream to GCS

    Arguments:
        bucket {str} -- GCS Bucket
        report_details {dict} -- Report definition
    """
    if not 'current_path' in report_details:
      return

    queue = Queue()

    report_id = report_details['id']
    chunk_size = self.chunk_multiplier * 1024 * 1024
    out_file = io.BytesIO()

    streamer = \
      ThreadedGCSObjectStreamUpload(
        client=Cloud_Storage.client(),
        creds=credentials.Credentials(
          email=self.email, project=self.project).get_credentials(),
        bucket_name=bucket,
        blob_name='{id}.csv'.format(id=report_id),
        chunk_size=chunk_size,
        streamer_queue=queue)
    streamer.start()

    with closing(urlopen(report_details['current_path'])) as _report:
      _downloaded = 0
      chunk_id = 1
      _report_size = int(_report.headers['content-length'])
      logging.info('Report is %s bytes', f'{_report_size:,}')
      while _downloaded < _report_size:
        chunk = _report.read(chunk_size)
        _downloaded += len(chunk)
        if _downloaded >= _report_size:
          # last chunk... trim to footer if there is one, or last blank line if not
          # NOTE: if no blank line (partial file?) NO TRIMMING WILL HAPPEN
          # THIS SHOULD NEVER BE THE CASE
          last = io.BytesIO(chunk)

          # find the footer
          blank_line_pos = chunk.rfind(b'\n\n')

          # if we don't find it, there's no footer.
          if blank_line_pos == -1:
            logging.error('No footer delimiter found. Writing entire final chunk as is.')
            queue.put(chunk)

          else:
            # read the footer
            last.seek(blank_line_pos)
            footer = last.readlines()
            group_count = sum(g.startswith(b'Group By:') for g in footer)
            total_block_start = chunk.rfind(b'\n' + b',' * group_count)

            if total_block_start == -1:
              last.truncate(blank_line_pos)

            else:
              last.truncate(total_block_start)

            queue.put(last.getvalue())
            # break

        else:
          queue.put(chunk)

        chunk_id += 1

    queue.join()
    streamer.stop()
