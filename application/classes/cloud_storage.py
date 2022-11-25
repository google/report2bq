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

import logging
from typing import Any, Dict

from auth.credentials import Credentials
from google.cloud import storage
from google.cloud.storage import Bucket


class Cloud_Storage(object):
  """Cloud Storage helper

  Handles the GCS operations for Report2BQ

  """

  def __init__(self, in_cloud=True, email: str = None, project: str = None):
    """constructor

    Keyword Arguments:
        in_cloud (bool):   (default: {True})
        email (str):  email address for the token (default: {None})
        project (str):  GCP project name (default: {None})
    """
    self.in_cloud = in_cloud
    self.email = email
    self.project = project

  @staticmethod
  def client(credentials: Credentials = None) -> storage.Client:
    return storage.Client(
        credentials=(credentials.credentials if credentials else None))

  @staticmethod
  def copy_to_gcs(bucket_name: str, report: Dict[str, Any],
                  credentials: Credentials = None):
    """copy from one bucket to another

    This is a copy from the bucket defined in the report definition (as DV360
    stores its reports in GCS) into the monitored bucket for upload. It's
    BLAZING fast, to the extent that there is essentially no limit on the
    maximum size of a DV360 report we can handle.

    The destination file name is the report's id.

    Arguments:
        bucket_name (str):  destination bucket name
        report (Dict[str, Any]):  report definition
    """
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    path_segments = report['current_path'].split('/')
    report_bucket = path_segments[-2]
    report_blob_name = path_segments[-1].split('?')[0]
    output_blob_name = report['id']

    source_bucket = Bucket(client, report_bucket)
    source_blob = source_bucket.blob(report_blob_name)

    destination_bucket = client.get_bucket(bucket_name)
    source_bucket.copy_blob(source_blob,
                            destination_bucket,
                            '{id}.csv'.format(id=output_blob_name))

    logging.info('File {report} copied from {source} to {bucket}.'.format(
        report=report_blob_name, bucket=bucket_name, source=report_bucket))

  @staticmethod
  def rename(bucket: str, source: str, destination: str,
             credentials: Credentials = None):
    """Rename a file.

    This is a copy/delete action as GCS has no actual rename option, however as
    it is all within GCS it is BLAZING fast, to the extent that there is
    essentially no limit on the maximum size of file we can rename.

    Arguments:
        bucket (str):  destination bucket name
        source (str):  current name
        destination (str):  new name
        credentials (Credentials):  authentication, if needed
    """
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    source_bucket = Bucket(client, name=bucket)
    source_blob = source_bucket.blob(blob_name=source)

    destination_bucket = client.get_bucket(bucket)
    source_bucket.copy_blob(source_blob,
                            destination_bucket, destination)
    source_blob.delete()

    logging.info(f'Renamed file %s as %s in %s.', bucket, source, destination)

  @staticmethod
  def fetch_file(bucket: str, file: str,
                 credentials: Credentials = None) -> str:
    """fetch a file from GCS

    Arguments:
      bucket (str):  bucket name
      file (str):  file name

    Returns:
      (str):  file content
    """
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    # logging.info('Fetching {f} from GCS'.format(f=file))

    try:
      content = client.get_bucket(bucket).blob(file).download_as_string()
    except Exception as ex:
      content = None
      logging.error('Error fetching file {f}\n{e}'.format(f=file, e=ex))

    return content

  @staticmethod
  def write_file(bucket: str, file: str, data: bytes,
                 credentials: Credentials = None) -> None:
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    # logging.info(f'Writing {file} to GCS: {len(data)}')

    try:
      client.get_bucket(bucket).blob(file).upload_from_string(data)
    except Exception as ex:
      content = None
      logging.error(f'Error writing file {file}\n{ex}')

  @staticmethod
  def read_first_line(report: dict, chunk: int = 4096,
                      credentials: Credentials = None) -> str:
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    header = client.read_chunk(
        report, chunk, credentials=credentials).split('\n')[0]
    return header

  @staticmethod
  def read_chunk(report: dict, chunk: int = 4096,
                 credentials: Credentials = None, start: int = 0) -> str:
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    path_segments = report['current_path'].split('/')
    report_bucket = path_segments[-2]
    report_blob_name = path_segments[-1].split('?')[0]

    source_bucket = Bucket(client, report_bucket)
    blob = source_bucket.blob(report_blob_name)

    data = blob.download_as_string(
        start=start, end=chunk, raw_download=True).decode('utf-8')
    return data

  @staticmethod
  def get_report_file(report: dict, credentials: Credentials = None) -> str:
    """get_report_file

    Find and return just the blob. We'll use this in DV360 to be able to stream
    the file in pieces so we can drop out the footer.

    Arguments:
        report (dict):  [description]

    Keyword Arguments:
        credentials (credentiala):  [description] (default: {None})

    Returns:
        str: [description]
    """
    client = storage.Client(
        credentials=(credentials.credentials if credentials else None))

    path_segments = report['current_path'].split('/')
    report_bucket = path_segments[-2]
    report_blob_name = path_segments[-1].split('?')[0]

    source_bucket = Bucket(client, report_bucket)
    blob = source_bucket.blob(report_blob_name)
    return blob
