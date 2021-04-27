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
#
from __future__ import annotations

import datetime
import io
import logging

from googleapiclient import http
from typing import Any, Dict, List, Tuple
from queue import Queue

from messytables.types import CellType

from classes import credentials
from classes import csv_helpers
from classes import Fetcher, ReportFetcher
from classes.cloud_storage import Cloud_Storage
from classes.decorators import retry
from classes.report_config import ReportConfig
from classes.services import Service
from classes.report_type import Type
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload


class DCM(ReportFetcher, Fetcher):
  report_type = Type.CM
  service_definition = Service.CM

  def __init__(self, email: str, profile: str, project: str) -> DCM:
    self.project = project
    self.email = email
    self.profile = profile

  def get_reports(self) -> List[Dict[str, Any]]:
    """Fetches the list of reports.

    Returns:
      result (Dict): the list of reports for the current user.
    """
    reports = []

    fields = {
      'sortField': 'LAST_MODIFIED_TIME',
      'sortOrder': 'DESCENDING',
      'fields': 'items(ownerProfileId,fileName,format,id,lastModifiedTime,'
                'name,schedule,type)',
      'profileId': self.profile,
    }

    result = self.fetch(
        self.service.reports().list,
        **fields
    )
    if 'items' in result:
      reports = [*reports, *result['items']]

    return reports

  def get_report_files(self, report_id: int) -> List[Dict[str, Any]]:
    """Fetches the list of the latest files for the report.

    A report CANNOT import if another is running. Since you cannot filter
    the list to just completed reports, I've upped the fetch to 5 (since
    only one can be running) which should be sufficient to find a single
    successfully completed report.

    Args:
      report_id: report id

    Returns:
      files (List[Dict[str, Any]]): List of latest DCM report files details
    """
    files = self.fetch(method=self.service.reports().files().list,
                       **{ 'profileId': self.profile,
                           'reportId': report_id,
                           'maxResults': '5',
                           'sortField': 'LAST_MODIFIED_TIME',
                           'sortOrder': 'DESCENDING',
                       })

    return files.get('items', [])

  def get_report_definition(self, report_id: int):
    """Fetches the dcm report definition.

    Args:
      report_id: report id

    Returns:
      List of latest DCM report files details
    """
    result = \
      self.fetch(method=self.service.reports().get,
                 **{ 'profileId': self.profile,
                     'reportId': report_id,
                     'fields':
                       'ownerProfileId,fileName,format,id,lastModifiedTime,'
                       'name,schedule,type',
                 }
    )

    return result

  def get_latest_report_file(self, report_id: int) -> Dict[str, Any]:
    """Fetches the most recent available dcm report file.

    Args:
      report_id: report id

    Returns:
      Available report file details
    """
    report = self.get_report_definition(report_id)
    files = self.get_report_files(report_id)

    for file in files:
      if file['status'] == 'REPORT_AVAILABLE':
        report['report_file'] = file
        report['profile_id'] = str(self.profile)
        break

    return report

  def normalize_report_details(self,
                               report_object: Dict[str, Any],
                               report_id: str) -> Dict[str, Any]:
    """Normalize api results into flattened data structure.

    Args:
      report_object: Report details from api queries method.
      report_id: the report id.

    Returns:
      Normalized data structure
    """
    if ('report_file' in report_object) and \
       (report_object['report_file'] is not None):
      gcs_path = report_object['report_file']['urls']['apiUrl']
      latest_runtime = report_object['report_file']['lastModifiedTime']

    else:
      gcs_path = ''
      latest_runtime = 0

    if 'schedule' in report_object:
      if report_object['schedule']['active'] is False:
        schedule_frequency = 'MANUAL'
      else:
        schedule_frequency = report_object['schedule']['repeats']
    else:
      schedule_frequency = 'MANUAL'

    # Normalize report data object
    report_data = {
      'id': report_object['id'],
      'profile_id': report_object['ownerProfileId'],
      'name': report_object['name'],
      'report_name': csv_helpers.sanitize_string(report_object['name']),
      'type': report_object['type'],
      'current_path': gcs_path,
      'last_updated':
          datetime.datetime.fromtimestamp(
            float(latest_runtime)/1000.0).strftime("%Y%m%d%H%M"),
      'update_cadence': schedule_frequency,
    }
    if 'report_file' in report_object:
      report_data['report_file'] = report_object['report_file']

    # Return
    return report_data

  def _find_first_data_byte(self, data: bytes) -> int:
    HEADER_MARKER=b'Report Fields\n'

    # Parse out the file; look for 'Report Fields\n'
    start = data.find(HEADER_MARKER) + len(HEADER_MARKER)
    return start

  def _read_data_chunk(self, report_data: ReportConfig,
                       chunk: int=16384) -> bytes:
    report_id = report_data.id
    file_id = report_data.report_file.id
    request = self.service.files().get_media(
      reportId=report_id, fileId=file_id)

    # Create a media downloader instance.
    out_file = io.BytesIO()
    downloader = http.MediaIoBaseDownload(out_file, request, chunksize=chunk)
    downloader.next_chunk()

    return out_file.getvalue()

  def read_header(self,
                  report_details: ReportConfig) -> Tuple[List[str],
                                                         List[CellType]]:
    """Reads the header of the report CSV file.

    Args:
        report_details (dict): the report definition

    Returns:
        Tuple[List[str], List[CellType]]: the csv headers and column types
    """
    if report_details.report_file:
      data = self._read_data_chunk(report_details, 163840)
      bytes_io = io.BytesIO(data)
      csv_start = self._find_first_data_byte(bytes_io.getvalue())
      bytes_io.seek(0 if csv_start == -1 else csv_start)
      return csv_helpers.get_column_types(io.BytesIO(bytes_io.read()))

    else:
      return (None, None)

  def stream_to_gcs(self, bucket: str, report_data: ReportConfig):
    """Streams the report CSV to Cloud Storage.

    Arguments:
        bucket (str):  GCS Bucket
        report_data (dict):  Report definition
    """
    if report_data.report_file:
      return

    queue = Queue()

    report_id = report_data.id
    file_id = report_data.report_file.id

    chunk_size = self.chunk_multiplier * 1024 * 1024
    out_file = io.BytesIO()

    download_request = self.service.files().get_media(
      reportId=report_id, fileId=file_id)
    downloader = http.MediaIoBaseDownload(
      out_file, download_request, chunksize=chunk_size)

    # Execute the get request and download the file.
    streamer = ThreadedGCSObjectStreamUpload(
      creds=credentials.Credentials(
        email=self.email, project=self.project).get_credentials(),
      client=Cloud_Storage.client(),
      bucket_name=bucket,
      blob_name='{id}.csv'.format(id=report_id),
      chunk_size=chunk_size,
      streamer_queue=queue)
    streamer.start()

    download_finished = False
    first = True
    while download_finished is False:
      status, download_finished = downloader.next_chunk()

      # Last chunk, drop the "Grand Total"
      if download_finished:
        total_pos = out_file.getvalue().rfind(b'Grand Total')
        if total_pos != -1:
          out_file.truncate(total_pos)

      # First chunk, skip the pre-header
      if first:
        csv_start = self._find_first_data_byte(out_file.getvalue())
        out_file.seek(0 if csv_start == -1 else csv_start)
        first = False
      else:
        out_file.seek(0)

      logging.info('Downloader status %s, %s of %s',
                   f'{(status.resumable_progress/status.total_size):3.2%}',
                   f'{status.resumable_progress:,}',
                   f'{status.total_size:,}')

      chunk = out_file.read(chunk_size)
      queue.put(chunk)
      out_file.seek(0)
      out_file.truncate(0)

    queue.join()
    streamer.stop()

  @retry(Exception, tries=3, delay=15, backoff=2)
  def run_report(self, report_id: int, synchronous: bool=False):
    request = self.service.reports().run(
      reportId=report_id, profileId=self.profile, synchronous=synchronous)
    result = request.execute()

    return result

  @retry(Exception, tries=3, delay=15, backoff=2)
  def report_state(self, report_id: int, file_id: int):
    request = self.service.reports().files().get(
      reportId=report_id, fileId=file_id, profileId=self.profile)
    result = request.execute()

    return result
