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
import inflection
import io
import logging
import os

from googleapiclient import http
from googleapiclient.discovery import Resource
from typing import Any, Dict, List, Tuple
from queue import Queue

from classes import Fetcher, ReportFetcher
from classes import credentials
from classes.cloud_storage import Cloud_Storage
from classes import csv_helpers
from classes.decorators import retry
from classes import discovery
from classes.firestore import Firestore
from classes.report_list import Report_List
from classes.services import Service
from classes.report_type import Type
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload

from google.cloud import pubsub


class DCM(ReportFetcher, Fetcher):
  report_type = Type.CM

  def __init__(self,
               email: str=None, profile: str=None, project: str=None) -> DCM:
    """
    Initialize Reporting Class
    """
    self.project = project
    self.email = email
    self.profile = profile
    self.report_list = Report_List()
    self.chunk_multiplier = int(os.environ.get('CHUNK_MULTIPLIER', 64))

  def service(self) -> Resource:
    return discovery.get_service(
      service=Service.CM,
      credentials=credentials.Credentials(email=self.email, project=self.project))

  def get_user_profiles(self):
    """
    Get list of user profiles
    Returns:
      list of user profiles
    """

    # Fetch User Profiles
    # https://developers.google.com/apis-explorer/#p/dfareporting/v3.1/dfareporting.accountUserProfiles.get
    result = self.fetch(method=self.service().userProfiles().list, **{
      # 'fields': 'items(profileId, accountName)'
    })
    return [
      item['profileId'] for item in result['items'] if not item['accountName'].startswith('SUPERUSER')
    ] if result and 'items' in result else []

  def get_reports(self) -> Report_List:
    """
    Fetches list of reports for current user
    Args:
      profile_id: profile id
    Returns:
      Report list
    """

    if not self.profile:
      profiles = self.get_user_profiles()

    else:
      profiles = [self.profile]
    # Fetch user reports
    # https://developers.google.com/apis-explorer/#p/dfareporting/v3.1/dfareporting.reports.list
    reports = []

    fields = {
      'sortField': 'LAST_MODIFIED_TIME',
      'sortOrder': 'DESCENDING',
      'fields': 'items(ownerProfileId,fileName,format,id,lastModifiedTime,'
                'name,schedule,type)',
    }

    for profile in profiles:
      fields['profileId'] = profile

      result = self.fetch(
          self.service().reports().list,
          **fields
      )
      if 'items' in result:
        reports = [*reports, *result['items']]

    return reports

  def get_report_files(self, report_id: int) -> List[Dict[str, Any]]:
    """
    Fetches latest dcm report files
    Args:
      report_id: report id
    Returns:
      List of latest DCM report files details
    """

    # Fetch some report files for specified report.
    # A report CANNOT import if another is running. Since you cannot filter
    # the list to just completed reports, I've upped the fetch to 5 (since
    # only one can be running) which should be sufficient to find a single
    # successfully completed report.
    files = self.fetch(
      method=self.service().reports().files().list,
      **{
        'profileId': self.profile,
        'reportId': report_id,
        'maxResults': '5',
        'sortField': 'LAST_MODIFIED_TIME',
        # 'fields': items(lastModifiedTime,reportId,status,urls/apiUrl)',
        'sortOrder': 'DESCENDING'
      }
    )

    return files

  def get_report_definition(self, report_id: int):
    """
    Fetches dcm report definition

    Args:
      profile_id: profile id
      report_id: report id

    Returns:
      List of latest DCM report files details
    """
    result = self.fetch(
      method=self.service().reports().get,
      **{
        'profileId': self.profile,
        'reportId': report_id,
        'fields':
          'ownerProfileId,fileName,format,id,lastModifiedTime,'
          'name,schedule,type',
      }
    )

    return result

  def extract_report_from_report_list(self, reports, report_id: int):
    """
    Pulls a report objects from api report list return
    Args:
      reports: Reports object returned from queries api call
      report_id: report id
    Returns:
      Report object
    """

    # Iterate through every report in list
    for report in reports:

      # Check for matching id
      if int(report['id']) == int(report_id):

        # Return report details
        return report

    # None found, return empty object
    return {}

  def get_latest_report_file(self, report_id: int):
    """
    Fetches most recent available dcm report file
    Args:
      profile_id: profile id
      report_id: report id
    Returns:
      Available report file details
    """
    report = self.get_report_definition(report_id)

    # Get latest file
    # Get recent report files
    files = self.get_report_files(report_id)

    # Return most recent file
    for file in files['items']:
      # Check if file is available
      if file['status'] == 'REPORT_AVAILABLE':
        # Append file details to report
        report['report_file'] = file

        # Append profile id to report
        report['profile_id'] = str(self.profile)

        # Return Report
        break

    # No files available
    return report

  def normalize_report_details(
    self,
    report_object: Dict[str, Any],
    report_id: str):
    """
    Normalize api results into flattened data structure
    Args:
      report_object: Report details from api queries method
    Returns:
      Normalized data structure
    """

    # Check if report has ever completed a run
    if ('report_file' in report_object) and (
      report_object['report_file'] is not None):
      # Exists
      gcs_path = report_object['report_file']['urls']['apiUrl']
      latest_runtime = report_object['report_file']['lastModifiedTime']

    else:
      # Report not yet run
      gcs_path = ''
      latest_runtime = 0

    # Check if schedule set
    if 'schedule' in report_object:
      # Normalize data
      if report_object['schedule']['active'] is False:
        # No Schedule
        schedule_frequency = 'MANUAL'
      else:
        # Scheduled
        schedule_frequency = report_object['schedule']['repeats']
    else:
      # Schedule Does Not exist
      schedule_frequency = 'MANUAL'

    # Normalize report data object
    report_data = {
      'id': report_object['id'],
      'profile_id': report_object['ownerProfileId'],
      'name': report_object['name'],
      'report_name': csv_helpers.sanitize_string(report_object['name']),
      'type': report_object['type'],
      'current_path': gcs_path,
      'last_updated': datetime.datetime.fromtimestamp(
          float(latest_runtime)/1000.0
      ).strftime("%Y%m%d%H%M"),
      'update_cadence': schedule_frequency,
    }
    if 'report_file' in report_object:
      report_data['report_file'] = report_object['report_file']

    # Return
    return report_data

  def fetch_report_config(self, report_object: Dict[str, Any], report_id: str):
    report_data = self.normalize_report_details(
      report_object=report_object, report_id=report_id)
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

  def add_report_to_config(self, report_data):
    """
    Add report to list of tracked reports
    Args:
      report_data: Normalized Report Data
    """

    # Add Report to List
    self.report_list.add_report(
        product = 'dcm',
        report_data = report_data
    )

  def get_report_details_from_config(self, report_id):
    """
    Fetches report details from config file
    Args:
      report_id: report_id
    Returns:
      Report details
    """

    # Get report details
    report_details = self.report_list.get_report('dcm', report_id)

    # Return
    return report_details

  def find_first_data_byte(self, data: bytes) -> int:
    HEADER_MARKER=b'Report Fields\n'

    # Parse out the file; look for 'Report Fields\n'
    start = data.find(HEADER_MARKER) + len(HEADER_MARKER)
    return start

  def read_data_chunk(self, report_data: dict, chunk: int=16384) -> bytes:
    report_id = report_data['id']
    file_id = report_data['report_file']['id']
    request = self.service().files().get_media(
      reportId=report_id, fileId=file_id)

    # Create a media downloader instance.
    out_file = io.BytesIO()
    downloader = http.MediaIoBaseDownload(out_file, request, chunksize=chunk)
    downloader.next_chunk()

    return out_file.getvalue()

  def read_header(self, report_details: dict) -> Tuple[List[str], List[str]]:
    if not 'report_file' in report_details:
      return (None, None)

    data = self.read_data_chunk(report_details, 163840)
    bytes_io = io.BytesIO(data)
    csv_start = self.find_first_data_byte(bytes_io.getvalue())
    if csv_start == -1:
      bytes_io.seek(0)
    else:
      bytes_io.seek(csv_start)

    return csv_helpers.get_column_types(io.BytesIO(bytes_io.read()))

  def stream_to_gcs(self, bucket: str, report_data: dict):
    """Multi-threaded stream to GCS

    Arguments:
        bucket {str} -- GCS Bucket
        report_data {dict} -- Report definition
    """
    if not 'report_file' in report_data:
      return

    queue = Queue()

    report_id = report_data['id']
    file_id = report_data['report_file']['id']

    chunk_size = self.chunk_multiplier * 1024 * 1024
    out_file = io.BytesIO()

    download_request = self.service().files().get_media(
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
    chunk_id = 0
    while download_finished is False:
      status, download_finished = downloader.next_chunk()

      # Last chunk, drop the "Grand Total" shit
      if download_finished:
        total_pos = out_file.getvalue().rfind(b'Grand Total')
        if total_pos != -1:
          out_file.truncate(total_pos)

      # First chunk, skip the pre-header shit
      if chunk_id == 0:
        csv_start = self.find_first_data_byte(out_file.getvalue())
        if csv_start == -1:
          out_file.seek(0)
        else:
          out_file.seek(csv_start)
      else:
        out_file.seek(0)

      logging.info('Downloader status %s, chunk %s (%s of %d)',
                   f'{(status.resumable_progress/status.total_size):3.2%}',
                   f'{status.resumable_progress:,}',
                   f'{status.total_size:,}',
                   chunk_id)

      chunk = out_file.read(chunk_size)
      # chunk = out_file.getvalue()
      queue.put(chunk)
      chunk_id += 1
      out_file.seek(0)
      out_file.truncate(0)

    queue.join()
    streamer.stop()

  @retry(Exception, tries=3, delay=15, backoff=2)
  def run_report(self, report_id: int, synchronous: bool=False):
    request = self.service().reports().run(
      reportId=report_id, profileId=self.profile, synchronous=synchronous)
    result = request.execute()

    return result

  @retry(Exception, tries=3, delay=15, backoff=2)
  def report_state(self, report_id: int, file_id: int):
    request = self.service().reports().files().get(
      reportId=report_id, fileId=file_id, profileId=self.profile)
    result = request.execute()

    return result

  def check_running_report(self, config: Dict[str, Any]):
    """Check a running CM report for completion

    Arguments:
        report {Dict[str, Any]} -- The report data structure from Firestore
    """
    append = config['append'] if config and 'append' in config else False
    response = self.report_state(report_id=config['id'], file_id=config['report_file']['id'])
    status = response['status'] if response and  'status' in response else 'UNKNOWN'

    logging.info('Report {report} status: {status}'.format(
      report=config['id'], status=status))
    firestore = Firestore(email=self.email, project=self.project)
    if status == 'REPORT_AVAILABLE':
      # Remove job from running
      firestore.remove_report_runner(config['id'])

      # Send pubsub to trigger report2bq now
      topic = 'projects/{project}/topics/report2bq-trigger'.format(
        project=self.project)
      ps = pubsub.PublisherClient()
      ps.publish(
        topic=topic, data=b'RUN', cm_id=config['id'],
        profile=config['profile_id'], email=config['email'],
        append=str(append), project=self.project
      )

    elif status == 'FAILED' or status =='CANCELLED':
      # Remove job from running
      logging.error(f'Report {config["id"]}: {inflection.humanize(status)}.')
      firestore.remove_report_runner(config['id'])
