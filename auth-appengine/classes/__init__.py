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
import traceback

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from typing import Any, Dict, List, Mapping

from classes.secret_manager_credentials import Credentials
from classes.decorators import lazy_property, retry
from classes.firestore import Firestore
from classes.gmail import GMail, GMailMessage
from classes.report_type import Type

class Fetcher(object):
  @retry(exceptions=HttpError, tries=3, backoff=2)
  def fetch(self, method, **kwargs: Mapping[str, str]) -> Dict[str, Any]:
    """Fetch results from a Resource connection.

    Args:
        method (class): method to execute.
        **kwargs (Dict[str, Any]): the Resource method arguments.

    Returns:
        Dict[str, Any]: results.
    """
    result = method(**kwargs).execute()
    return result


class ReportFetcher(object):
  report_type: Type  = None

  def read_header(self, report_details: dict) -> list:
    pass

  def stream_to_gcs(self, bucket: str, report_details: Dict[str, Any]) -> None:
    pass

  def normalize_report_details(self, report_object: Dict[str, Any], report_id: str):
    pass

  def fetch_report_config(self, report_object: Dict[str, Any], report_id: str):
    pass

  def get_latest_report_file(self, report_id: str):
    pass

  def run_report(self, report_id: int):
    pass

  def check_running_report(self, config: Dict[str, Any]):
    pass

  def get_reports(self) -> Dict[str, Any]:
    pass

  def service(self) -> Resource:
    pass


class ReportRunner(object):
  report_type = None
  project = None
  email = None

  @lazy_property
  def firestore(self) -> Firestore:
    return Firestore(project=self.project, email=self.email)

  def run(self, unattended: bool):
    """Run the report.

    Args:
        unattended (bool): log the report for later or wait for the result
    """
    pass

  def _email_error(self,
                   message: str,
                   email: str=None,
                   error: Exception=None) -> None:
    """Email the error to the administrator

    Send an email (with errors) to the administrator and/or job owner.

    Args:
        message (str): the message.
        email (str, optional): job owner's email. Defaults to None.
        error (Exception, optional): any error found. Defaults to None.
    """
    _to = [email] if email else []
    _administrator = \
      os.environ.get('ADMINISTRATOR_EMAIL') or self.FIRESTORE.get_document(
        Type._ADMIN, 'admin').get('email')
    _cc = [_administrator] if _administrator else []

    if _trace := \
    ''.join(traceback.TracebackException.from_exception(error).format()) \
      if error else None:
      _trace = 'Error\n\n' + _trace

    if _to or _cc:
      message = GMailMessage(
        to=_to,
        cc=_cc,
        subject=f'Error in report_loader',
        body=f'{message}{_trace if _trace else ""}',
        project=os.environ.get('GCP_PROJECT'))

      GMail().send_message(
        message=message,
        credentials=Credentials(
          email=email, project=os.environ.get('GCP_PROJECT'))
      )
