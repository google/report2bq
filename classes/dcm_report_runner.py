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

from classes.dcm import DCM
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.firestore import Firestore
from io import StringIO


class DCMReportRunner(object):
  def __init__(self, cm_ids: list=None, profile: str=None,
               account_id: int=None, superuser: bool=False, email: str=None, 
               synchronous: bool=False, project: str=None):
    self.email = email
    self.cm_ids = cm_ids
    self.cm_profile = profile
    self.account_id = account_id
    self.superuser = superuser
    self.synchronous = synchronous
    self.project = project
    self.firestore = Firestore(email=email, project=project)


  def run(self, unattended: bool=False) -> None:
    dcm = DCM(email=self.email, superuser=self.superuser, project=self.project)
    if unattended:
      self._unattended_run(dcm)
    else:
      self._attended_run(dcm)


  def _attended_run(self, dcm: DCM) -> None:
    successful = []
    for cm_id in self.cm_ids:
      response = dcm.run_report(profile_id=self.cm_profile, report_id=cm_id, account_id=self.account_id, synchronous=self.synchronous)
      if response:
        buffer = StringIO()
        pprint.pprint(response, stream=buffer)
        logging.info(buffer.getvalue())

      while response['status'] == 'PROCESSING':
        time.sleep(60 * 0.5)
        response = dcm.report_state(profile_id=self.cm_profile, report_id=cm_id, file_id=response['id'], account_id=self.account_id)
        buffer = StringIO()
        pprint.pprint(response, stream=buffer)
        logging.info(buffer.getvalue())

      successful.append(cm_id)

    report2bq = Report2BQ(cm=True, cm_ids=successful, email=self.email, in_cloud=True, project=self.project, profile=self.cm_profile)
    report2bq.handle_cm_reports()


  def _unattended_run(self, dcm: DCM) -> None:
    for cm_id in self.cm_ids:
      response = dcm.run_report(profile_id=self.cm_profile, report_id=cm_id, account_id=self.account_id, synchronous=self.synchronous)
      if response:
        buffer = StringIO()
        pprint.pprint(response, stream=buffer)
        logging.info(buffer.getvalue())
        break

      runner = {
        'type': Type.CM.value,
        'project': self.project,
        'report_id': cm_id,
        'email': self.email,
        'profile': self.cm_profile,
        'account_id': self.account_id,
        'superuser': self.superuser,
        'file_id': response['id']
      }
      self.firestore.store_report_runner(runner)
