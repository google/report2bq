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
import datetime
import httplib2
import io
import logging
import mmap
import os
import re
import sys
import time

# Discovery Service Import
from apiclient import discovery

# Class Imports
from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials
from classes.csv_helpers import CSVHelpers
from classes.threaded_streamer import ThreadedGCSObjectStreamUpload

# Other imports
from queue import Queue, Empty
from typing import Dict, Any


class DBM(object):

  def __init__(self, email: str, project: str):
    """
    Initialize Reporting Class
    """
    # Get authorized http transport
    self.credentials = Credentials(email=email, project=project)

    # Create service for api calls
    self.dbm_service = discovery.build(
        "doubleclickbidmanager",
        "v1",
        credentials = self.credentials.get_credentials(),
        cache_discovery = False
    )

    # http transport
    self.http = httplib2.Http()

    # Oauth Headers
    self.oauth_headers = self.credentials.get_auth_headers()


  def get_reports(self):
    """
    Fetches list of reports for current user
    Returns:
      Report list
    """

    # Use Discovery Service to make api call
    # https://developers.google.com/apis-explorer/#p/doubleclickbidmanager/v1.1/doubleclickbidmanager.queries.listqueries
    reports = self.dbm_service.queries().listqueries(
        fields = 'queries(metadata(googleCloudStoragePathForLatestReport,latestReportRunTimeMs,title),params/type,queryId,schedule/frequency)'
    )

    # Execute request
    result = reports.execute()

    # Return results
    return result


  def get_latest_report_file(self, report_id):
    """
    Pulls a report objects from api report list return
    Args:
      reports: Reports object returned from queries api call
      report_id: report id
    Returns:
      Report object
    """

    reports = self.dbm_service.reports().listreports(queryId=report_id)

    # Execute request
    results = reports.execute()
    if results:
      if 'reports' in results:
        ordered = sorted(results['reports'], key=lambda k: int(k['metadata']['status']['finishTimeMs']))
        return ordered[-1]
      else:
        logging.info('No reports - has this report run successfully yet?')

    # None found, return empty object
    return {}


  def normalize_report_details(self, report_object):
    """
    Normalize api results into flattened data structure
    Args:
      report_object: Report details from api queries method
    Returns:
      Normalized data structure
    """
    # Fetch query details too, as this contains the main piece
    query_object = self.dbm_service.queries().getquery(queryId=report_object['key']['queryId']).execute()

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
      'table_name': re.sub('[^a-zA-Z0-9]+', '_', query_object['metadata']['title']),
      'type': query_object['params']['type'],
      'current_path': gcs_path,
      'last_updated': datetime.datetime.fromtimestamp(
          float(latest_runtime)/1000.0
      ).strftime("%Y%m%d%H%M"),
      'update_cadence': query_object['schedule']['frequency'],
    }

    # Return
    return report_data


  def run_report(self, report_id: int, retry: int=0):
    if retry < 5:
      try:
        request = self.dbm_service.queries().runquery(queryId=report_id)
        result = request.execute()

      except Exception as e:
        retry += 1
        logging.error('Error {err} caught: backing off for {retry} minutes and retrying.'.format(err=e, retry=retry))
        time.sleep(60 * retry)
        return self.run_report(report_id, retry)
    
    else:
      raise Exception('Max retries exceeded')
    
    return result


  def report_state(self, report_id: int, retry: int=0):
    if retry < 5:
      try:
        request = self.dbm_service.reports().listreports(queryId=report_id)
        results = request.execute()

        if results:
          ordered = sorted(results['reports'], key=lambda k: int(k['metadata']['reportDataStartTimeMs']))
          return ordered[-1]['metadata']['status']['state']

      except Exception as e:
        retry += 1
        logging.error('Error {err} caught: backing off for {retry} minutes and retrying.'.format(err=e, retry=retry))
        time.sleep(60 * retry)
        return self.report_state(report_id=report_id, retry=retry)
    
    else:
      raise Exception('Max retries exceeded')
    
    return 'UNKNOWN'


  def read_header(self, report_details: dict) -> list:
    data = Cloud_Storage.read_chunk(report_details, 16384, self.credentials)
    bytes_io = io.BytesIO(bytes(data, 'utf-8'))

    return CSVHelpers.get_column_types(bytes_io)


  def stream_to_gcs(self, bucket: str, report_data: Dict[str, Any]) -> None:
    """Multi-threaded stream to GCS
    
    Arguments:
        bucket {str} -- GCS Bucket
        report_data {dict} -- Report definition
    """
    queue = Queue()

    report_id = report_data['id']
    chunk_size = 16 * 1024 * 1024
    out_file = io.BytesIO()

    streamer = ThreadedGCSObjectStreamUpload(client=Cloud_Storage.client(credentials=self.credentials), 
                                             bucket_name=bucket,
                                             blob_name='{id}.csv'.format(id=report_id), 
                                             chunk_size=chunk_size, 
                                             queue=queue)
    streamer.start()

    _report = Cloud_Storage.get_report_file(report=report_data, credentials=self.credentials)
    _report.reload()
    _downloaded = 0
    chunk_id = 0
    while _downloaded < _report.size:
      chunk = _report.download_as_string(start=_downloaded, end=_downloaded + chunk_size, raw_download=True)
      _report_size = _report.size
      _downloaded += len(chunk)
      if _downloaded >= _report.size:
        # last chunk...
        last = io.BytesIO(chunk)

        # lose the footer
        total_pos = last.getvalue().rfind(b'Report Time')
        if total_pos != -1:
          last.truncate(total_pos)

        # now the grand total, which for DV360 is not titled
        # this will always have no dimensions, so begins a line with a ','
        newline_pos = last.getvalue().rfind(b'\n,') #, comma_pos)
        if newline_pos != -1:
          last.truncate(newline_pos)

        queue.put((chunk_id, last.getvalue()))
        break

      else:
        queue.put((chunk_id, chunk))

      chunk_id += 1

    queue.join()
    streamer.stop()
