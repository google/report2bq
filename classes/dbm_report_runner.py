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

from classes.dbm import DBM
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.firestore import Firestore
from io import StringIO


class DBMReportRunner(object):
  def __init__(self, dbm_ids: list=None,
               email: str=None, synchronous: bool=False, project: str=None):
    self.email = email
    self.dbm_ids = dbm_ids
    self.synchronous = synchronous
    self.project = project
    self.firestore = Firestore(email=email, project=project)


  def run(self, unattended: bool=False):
    dbm = DBM(email=self.email, project=self.project)

    if unattended:
      self._unattended_run(dbm)
    else:
      self._attended_run(dbm)

  def _attended_run(self, dbm: DBM) -> None:
    successful = []

    for dbm_id in self.dbm_ids:
      response = dbm.run_report(dbm_id)
      if response:
        buffer = StringIO()
        pprint.pprint(response, stream=buffer)
        logging.info(buffer.getvalue())
        break

      while True:
        status = dbm.report_state(dbm_id)
        logging.info('Report {report} status: {status}'.format(report=dbm_id, status=status))
        if status == 'RUNNING':
          time.sleep(10)
        elif status == 'DONE':
          successful.append(dbm_id)
          break
        else:
          break
    
    report2bq = Report2BQ(
      dv360=True, dv360_ids=successful, email=self.email, in_cloud=True, project=self.project
    )
    report2bq.handle_dv360_reports()


  def _unattended_run(self, dbm: DBM) -> None:
    for dbm_id in self.dbm_ids:
      response = dbm.run_report(dbm_id)
      if response:
        buffer = StringIO()
        pprint.pprint(response, stream=buffer)
        logging.error(buffer.getvalue())
        break 

      runner = {
        'type': Type.DV360.value,
        'project': self.project,
        'report_id': dbm_id,
        'email': self.email,
      }
      self.firestore.store_report_runner(runner)

      
