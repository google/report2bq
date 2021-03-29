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
from __future__ import annotations

import logging
import pprint
import time

from classes import ReportRunner
from classes.dbm import DBM
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.firestore import Firestore
from io import StringIO


class DBMReportRunner(ReportRunner):
  report_type = Type.DV360

  def __init__(self, dbm_id: str=None,
               email: str=None, project: str=None, **unused) -> DBMReportRunner:
    """Initialize the runner.

    The runner inherits from ReportRunner, which mandates the 'run' method.

    Args:
        fbm_id (str, optional): DV360 report id. Defaults to None.
        email (str, optional): User email for the token. Defaults to None.
        project (str, optional): Project. Defaults to None but should be
          pre-populated with the current project by the caller.
        **unused (dict): Any other arguments.

    Returns:
        DBMReportRunner: self
    """
    self.email = email
    self.dbm_id = dbm_id
    self.project = project

  def run(self, unattended: bool=True):
    """Perform the report run

    Args:
        unattended (bool, optional): Is this a fire and forget (True) or wait
          for the report to complete (False). Defaults to True.
    """
    dbm = DBM(email=self.email, project=self.project)

    if unattended:
      self._unattended_run(dbm)
    else:
      self._attended_run(dbm)

  def _attended_run(self, dbm: DBM) -> None:
    """_attended_run.

    Run the report and wait for it to finish.

    Args:
        dbm (DBM): The DV360 controller.
    """
    response = dbm.run_report(self.dbm_id)
    if response:
      buffer = StringIO()
      pprint.pprint(response, stream=buffer)
      logging.info(buffer.getvalue())

    while True:
      status = dbm.report_state(self.dbm_id)
      logging.info(f'Report {self.dbm_id} status: {status}')
      if status == 'RUNNING':
        time.sleep(10)

      elif status == 'DONE':
        report2bq = Report2BQ(
          dv360=True, dv360_id=self.dbm_id, email=self.email,
          project=self.project
        )
        report2bq.handle_report_fetcher(fetcher=dbm, report_id=self.dbm_id)
        break

      else:
        logging.error(f'DV360 Report {self.dbm_id} failed to run: {status}')
        break

  def _unattended_run(self, dbm: DBM) -> None:
    """_unattended_run.

    Start the report running and store the run configuration in Firestore. This
    will then be monitored for completion and import by the run-monitor.

    Args:
        dbm (DBM): The DV360 controller.
    """
    response = dbm.run_report(self.dbm_id)
    if response:
      runner = {
        'type': Type.DV360.value,
        'project': self.project,
        'report_id': self.dbm_id,
        'email': self.email,
      }
      self.firestore.store_report_runner(runner)


