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
import os
import random
import re
import sys
import time

# Discovery Service Import
from apiclient import discovery
from googleapiclient import http

# Class Imports
from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials
from classes.csv_helpers import CSVHelpers
from classes.gcs_streamer import GCSObjectStreamUpload
from classes.report_list import Report_List
from classes.threaded_streamer import ThreadedGCSObjectStreamUpload

# Other imports
from queue import Queue, Empty


class DCM(object):

  def __init__(self, email: str=None, superuser: bool=False, project: str=None):
    """
    Initialize Reporting Class
    """
    self.project = project
    self.superuser = superuser
    self.email = email

    # Get authorized http transport
    self.credentials = Credentials(email=email, project=project)

    # Superuser
    self.superuser = superuser
    if superuser:
      version = 'internalv3.3'
    else:
      version = 'v3.3'

    # Create service for api calls
    self.dcm_service = discovery.build(
        "dfareporting",
        version,
        credentials = self.credentials.get_credentials(),
        cache_discovery = False
    )

    # Init Report List Controller
    self.report_list = Report_List()

    # http transport
    self.http = httplib2.Http()

    # Oauth Headers
    self.oauth_headers = self.credentials.get_auth_headers()


  def get_user_profiles(self):
    """
    Get list of user profiles
    Returns:
      list of user profiles
    """

    # Fetch User Profiles
    # https://developers.google.com/apis-explorer/#p/dfareporting/v3.1/dfareporting.accountUserProfiles.get
    profiles = self.dcm_service.userProfiles().list()

    # Execute request
    result = profiles.execute()

    # Return result
    return result


  def get_reports(self, profile_id: int, account_id: int=None) -> Report_List:
    """
    Fetches list of reports for current user
    Args:
      profile_id: profile id
    Returns:
      Report list
    """

    # Fetch user reports
    # https://developers.google.com/apis-explorer/#p/dfareporting/v3.1/dfareporting.reports.list
    if self.superuser:
      reports = self.dcm_service.reports().list(
          profileId = profile_id,
          accountId=account_id,
          sortField = 'LAST_MODIFIED_TIME',
          sortOrder = 'DESCENDING',
          fields = 'items(accountId,fileName,format,id,lastModifiedTime,name,schedule,type)'
      )
    else:
      reports = self.dcm_service.reports().list(
          profileId = profile_id,
          sortField = 'LAST_MODIFIED_TIME',
          sortOrder = 'DESCENDING',
          fields = 'items(accountId,fileName,format,id,lastModifiedTime,name,schedule,type)'
      )

    # Execute request
    result = reports.execute()

    # Return result
    return result


  def get_report_files(self, profile_id, report_id, account_id=None):
    """
    Fetches latest dcm report files
    Args:
      profile_id: profile id
      report_id: report id
    Returns:
      List of latest DCM report files details
    """

    # Fetch report files for specified report
    if self.superuser:
      files = self.dcm_service.reports().files().list(
        profileId = profile_id,
        accountId = account_id,
        reportId = report_id,
        maxResults = '5',
        sortField = 'LAST_MODIFIED_TIME',
        # fields = 'items(lastModifiedTime,reportId,status,id,urls/apiUrl)',
        sortOrder = 'DESCENDING'
      )
    else:
      files = self.dcm_service.reports().files().list(
        profileId = profile_id,
        reportId = report_id,
        maxResults = '5',
        sortField = 'LAST_MODIFIED_TIME',
        # fields = 'items(lastModifiedTime,reportId,status,urls/apiUrl)',
        sortOrder = 'DESCENDING'
      )

    # Execute request
    result = files.execute()

    # Return result
    return result


  def get_report_definition(self, profile_id, report_id, account_id=None):
    """
    Fetches dcm report definition

    Args:
      profile_id: profile id
      report_id: report id

    Returns:
      List of latest DCM report files details
    """

    # Fetch report definition for specified report
    if self.superuser:
      fetcher = self.dcm_service.reports().get(
        profileId = profile_id,
        accountId = account_id,
        reportId = report_id
      )
    else:
      fetcher = self.dcm_service.reports().get(
        profileId = profile_id,
        reportId = report_id
      )

    # Execute request
    result = fetcher.execute()

    # Return result
    return result


  def extract_report_from_report_list(self, reports, report_id):
    """
    Pulls a report objects from api report list return
    Args:
      reports: Reports object returned from queries api call
      report_id: report id
    Returns:
      Report object
    """

    # Iterate through every report in list
    for report in reports['items']:

      # Check for matching id
      if int(report['id']) == int(report_id):

        # Return report details
        return report

    # None found, return empty object
    return {}


  def get_latest_report_file(self, profile_id: int, report_id: int, account_id: int=None):
    """
    Fetches most recent available dcm report file
    Args:
      profile_id: profile id
      report_id: report id
    Returns:
      Available report file details
    """

    # List reports
    reports = self.get_reports(profile_id, account_id)

    # Extract report details
    report = self.extract_report_from_report_list(
        reports=reports,
        report_id=report_id
    )

    # Get latest file
    # Get recent report files
    files = self.get_report_files(profile_id, report_id, account_id)

    # Return most recent file
    for file in files['items']:
      # Check if file is available
      if file['status'] == 'REPORT_AVAILABLE':
        # Append file details to report
        report['report_file'] = file

        # Append profile id to report
        report['profile_id'] = str(profile_id)

        # Return Report
        break

    # No files available
    return report


  def normalize_report_details(self, report_object):
    """
    Normalize api results into flattened data structure
    Args:
      report_object: Report details from api queries method
    Returns:
      Normalized data structure
    """

    # Check if report has ever completed a run
    if report_object['report_file'] is not None:

      # Exists
      gcs_path = report_object['report_file']['urls']['apiUrl']
      latest_runtime = report_object['report_file']['lastModifiedTime']

    else:

      # Report not yet run
      gcs_path = ""
      latest_runtime = 0

    # Check if schedule set
    if 'schedule' in report_object:

      # Normalize data
      if report_object['schedule']['active'] is False:

        # No Schedule
        schedule_frequency = "ONE_TIME"
      else:

        # Scheduled
        schedule_frequency = report_object['schedule']['repeats']
    else:

      # Schedule Does Not exist
      schedule_frequency = "ONE_TIME"

    # Normalize report data object
    report_data = {
      'id': report_object['id'],
      'profile_id': report_object['profile_id'],
      'name': report_object['name'],
      'table_name': re.sub('[^a-zA-Z0-9]+', '_', report_object['name']),
      'type': report_object['type'],
      'current_path': gcs_path,
      'last_updated': datetime.datetime.fromtimestamp(
          float(latest_runtime)/1000.0
      ).strftime("%Y%m%d%H%M"),
      'update_cadence': schedule_frequency,
      'report_file': report_object['report_file']
    }

    # Return
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
    request = self.dcm_service.files().get_media(reportId=report_id, fileId=file_id)

    # Create a media downloader instance.
    out_file = io.BytesIO()
    downloader = http.MediaIoBaseDownload(out_file, request, chunksize=chunk)
    downloader.next_chunk()

    return out_file.getvalue()


  def read_header(self, report_details: dict) -> list:
    data = self.read_data_chunk(report_details, 163840)
    bytes_io = io.BytesIO(data)
    csv_start = self.find_first_data_byte(bytes_io.getvalue())
    if csv_start == -1:
      bytes_io.seek(0)
    else:
      bytes_io.seek(csv_start)

    return CSVHelpers.get_column_types(io.BytesIO(bytes_io.read()))
    
    
  def _stream_to_gcs(self, bucket: str, report_data: dict):
    """Multi-threaded stream to GCS
    
    Arguments:
        bucket {str} -- GCS Bucket
        report_data {dict} -- Report definition
    """
    queue = Queue()

    report_id = report_data['id']
    file_id = report_data['report_file']['id']

    chunk_size = 16 * 1024 * 1024
    out_file = io.BytesIO()

    download_request = self.dcm_service.files().get_media(reportId=report_id, fileId=file_id)
    downloader = http.MediaIoBaseDownload(out_file, download_request, chunksize=chunk_size)

    # Execute the get request and download the file.
    streamer = ThreadedGCSObjectStreamUpload(client=Cloud_Storage.client(credentials=self.credentials), 
                                             bucket_name=bucket,
                                             blob_name='{id}.csv'.format(id=report_id), 
                                             chunk_size=chunk_size, 
                                             queue=queue)
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

      logging.info('Downloader status {percent:3.2%}, chunk {chunk} ({progress} of {size})'.format(
        percent=(status.resumable_progress/status.total_size), progress=status.resumable_progress,
        size=status.total_size, chunk=chunk_id)
      )
      
      chunk = out_file.read(chunk_size)
      # chunk = out_file.getvalue()
      queue.put((chunk_id, chunk))
      chunk_id += 1
      out_file.seek(0)
      out_file.truncate(0)

    queue.join()
    streamer.stop()


  def run_report(self, profile_id: int, report_id: int, account_id: int=None, synchronous: bool=False, retry: int=0):
    if retry < 5:
      try:
        if self.superuser:
          request = self.dcm_service.reports().run(reportId=report_id, profileId=profile_id, accountId=account_id, synchronous=synchronous)
        else:
          request = self.dcm_service.reports().run(reportId=report_id, profileId=profile_id, synchronous=synchronous)
      
        result = request.execute()

      except Exception as e:
        retry += 1
        logging.error('Error {err} caught: backing off for {retry} minutes and retrying.'.format(err=e, retry=retry))
        time.sleep(60 * retry)
        return self.run_report(profile_id, report_id, account_id, synchronous, retry)
    
    else:
      raise Exception('Max retries exceeded')
    
    return result

  def report_state(self, profile_id: int, report_id: int, file_id: int, account_id: int=None, retry: int=0):
    if retry < 5:
      try:
        if self.superuser:
          request = self.dcm_service.reports().files().get(reportId=report_id, fileId=file_id, profileId=profile_id, accountId=account_id)
        else:
          request = self.dcm_service.reports().files().get(reportId=report_id, fileId=file_id, profileId=profile_id)
    
        result = request.execute()

      except Exception as e:
        retry += 1
        logging.error('Error {err} caught: backing off for {retry} minutes and retrying.'.format(err=e, retry=retry))
        time.sleep(60 * retry)
        return self.report_state(
          profile_id=profile_id, report_id=report_id, account_id=account_id, file_id=file_id, retry=retry
        )
    
    else:
      raise Exception('Max retries exceeded')
    
    return result
