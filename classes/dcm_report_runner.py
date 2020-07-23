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

# Python logging
import logging
import pprint
import time

from classes import ReportRunner
from classes.dcm import DCM
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.firestore import Firestore
from io import StringIO


class DCMReportRunner(ReportRunner):
  report_type = Type.CM

  def __init__(self, cm_id: str=None, profile: str=None,
               email: str=None, project: str=None):
    self.email = email
    self.cm_id = cm_id
    self.cm_profile = profile
    self.project = project
    self.firestore = Firestore(email=email, project=project)


  def run(self, unattended: bool=True) -> None:
    dcm = DCM(email=self.email, project=self.project, profile=self.cm_profile)
    
    if unattended:
      self._unattended_run(dcm)
    else:
      self._attended_run(dcm)


  def _attended_run(self, dcm: DCM) -> None:
    successful = []
    response = dcm.run_report(report_id=self.cm_id, synchronous=True)
    if response:
      buffer = StringIO()
      pprint.pprint(response, stream=buffer)
      logging.info(buffer.getvalue())

    while response['status'] == 'PROCESSING':
      time.sleep(60 * 0.5)
      response = dcm.report_state(report_id=self.cm_id, file_id=response['id'])
      buffer = StringIO()
      pprint.pprint(response, stream=buffer)
      logging.info(buffer.getvalue())

    report2bq = Report2BQ(
      cm=True, cm_id=self.cm_id, email=self.email, project=self.project,
      profile=self.cm_profile
    )
    report2bq.handle_report_fetcher(fetcher=dcm, report_id=self.cm_id)


  def _unattended_run(self, dcm: DCM) -> None:
    response = dcm.run_report(report_id=self.cm_id, synchronous=False)
    if response:
      buffer = StringIO()
      pprint.pprint(response, stream=buffer)
      logging.info(buffer.getvalue())

      runner = {
        'type': Type.CM.value,
        'project': self.project,
        'report_id': self.cm_id,
        'email': self.email,
        'profile': self.cm_profile,
        'file_id': response['id']
      }
      self.firestore.store_report_runner(runner)
