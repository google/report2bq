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

import os

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from messytables import types
from typing import Any, Dict, List, Mapping, Tuple

from classes import credentials
from classes import decorators
from classes import discovery
from classes import firestore
from classes import gmail
from classes import report_type
from classes import services

class Fetcher(object):
  @decorators.retry(exceptions=HttpError, tries=3, backoff=2)
  def fetch(self, method, **kwargs: Mapping[str, str]) -> Dict[str, Any]:
    result = method(**kwargs).execute()
    return result


class ReportFetcher(object):
  report_type: report_type.Type
  service_definition: services.Service

  chunk_multiplier = int(os.environ.get('CHUNK_MULTIPLIER', 64))
  email = None
  project = None
  profile = None

  @decorators.lazy_property
  def service(self) -> Resource:
    """Creates the API service for the product.

    Returns:
        Resource: the service definition
    """
    return discovery.get_service(
      service=self.service_definition,
      credentials=credentials.Credentials(email=self.email,
                                          project=self.project))

  def read_header(self, report_details: dict) \
    -> Tuple[List[str], List[types.CellType]]:
    """Reads the header of the report CSV file.

    Args:
        report_details (dict): the report definition

    Returns:
        Tuple[List[str], List[CellType]]: the csv headers and column types
    """
    pass

  def stream_to_gcs(self, bucket: str, report_details: Dict[str, Any]) -> None:
    """Streams the report CSV to Cloud Storage.

    Args:
      bucket {str} -- GCS Bucket
      report_data {dict} -- Report definition
    """
    pass

  def normalize_report_details(self,
                               report_object: Dict[str, Any],
                               report_id: str) -> Dict[str, Any]:
    """Normalizes the api format report into a flattened data structure.

    Args:
      report_object: Report details from api queries method
      report_id: the report id.

    Returns:
      result (Dict): the normalized data structure
    """
    pass

  def fetch_report_config(self, report_object: Dict[str, Any],
                          report_id: str) -> Dict[str, Any]:
    """Fetches a report configuration.

    This fetched the latest version of a report's configuration from the
    product, normalizes it fo the format that Report2BQ wants, and merges in
    the Report2BQ state fields.

    Args:
        report_object (Dict[str, Any]): the existing report object
        report_id (str): the report id

    Returns:
        Dict[str, Any]: the updated configuration
    """
    report_data = self.normalize_report_details(report_object=report_object,
                                                report_id=report_id)
    keys_to_update = [
      'email', 'dest_dataset', 'dest_project', 'dest_table', 'notifier',
      'schema', 'append', 'force', 'infer_schema' ]

    for key in keys_to_update:
      if key in report_object:
        report_data[key] = report_object[key]

    return report_data

  def get_latest_report_file(self, report_id: str) -> Dict[str, Any]:
    """Fetch the last known successful report's definition.

    Args:
      report_id: report id
    Returns:
      result (Dict): the last known report, or an empty Dict if it has
                     not yet run.
    """
    pass

  def run_report(self, report_id: int,
                 asynchronous: bool=True) -> Dict[str, Any]:
    """Runs a report on the product.

    Args:
        report_id (int): the report to run.
        asynchronous (bool): fire and forget or wait for the result.

    Returns:
        Dict[str, Any]: the run result
    """
    pass

  def check_running_report(self, config: Dict[str, Any]):
    pass

  def get_reports(self) -> Dict[str, Any]:
    """Fetches a list of reports for current user.

    Returns:
      result (Dict): the list of reports for the current user.
    """
    pass


class ReportRunner(object):
  report_type = None
  project = None
  email = None

  @decorators.lazy_property
  def firestore(self) -> firestore.Firestore:
    return firestore.Firestore(project=self.project, email=self.email)

  def run(self, unattended: bool):
    """Runs the report.

    Args:
        unattended (bool): wait for the result or just run and log for the run
                           monitor.
    """
    pass

  def _email_error(self,
                   message: str,
                   email: str=None,
                   error: Exception=None) -> None:
    """Emails the error to the owner, and the administrator if defined.

    Args:
        message (str): the message
        email (str, optional): report owner email. Defaults to None.
        error (Exception, optional): any error. Defaults to None.
    """
    to = [email] if email else []
    administrator = \
      os.environ.get('ADMINISTRATOR_EMAIL') or \
        self.FIRESTORE.get_document(report_type.Type._ADMIN,
                                    'admin').get('email')
    cc = [administrator] if administrator else []

    if to or cc:
      body=f'{message}{gmail.error_to_trace(error)}'
      message = gmail.GMailMessage(to=to,
                                   cc=cc,
                                   subject=f'Error in report_loader',
                                   body=body,
                                   project=self.project)

      gmail.send_message(message=message,
                         credentials=credentials.Credentials(
                           email=email, project=self.project))
