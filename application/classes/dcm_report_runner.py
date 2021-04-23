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
from classes.dcm import DCM
from classes.report2bq import Report2BQ
from classes.report_type import Type
from io import StringIO


class DCMReportRunner(ReportRunner):
  """DCMReportRunner.

  Run CM360 reports on demand.
  """
  report_type = Type.CM

  def __init__(self, cm_id: str=None, profile: str=None,
               email: str=None, project: str=None, **unused) -> DCMReportRunner:
    """Initialize the runner.

    The runner inherits from ReportRunner, which mandates the 'run' method.

    Args:
        cm_id (str, optional): CM report id. Defaults to None.
        profile (str, optional): User's CM profile id. Defaults to None.
        email (str, optional): User email for the token. Defaults to None.
        project (str, optional): Project. Defaults to None but should be
          pre-populated with the current project by the caller.

    Returns:
        DCMReportRunner: self
    """
    self.email = email
    self.cm_id = cm_id
    self.cm_profile = profile
    self.project = project

  def run(self, unattended: bool=True) -> None:
    """Perform the report run

    Args:
        unattended (bool, optional): Is this a fire and forget (True) or wait
          for the report to complete (False). Defaults to True.
    """
    dcm = DCM(email=self.email, project=self.project, profile=self.cm_profile)

    if unattended:
      self._unattended_run(dcm)
    else:
      self._attended_run(dcm)

  def _attended_run(self, dcm: DCM) -> None:
    """_attended_run.

    Run the report and wait for it to finish.

    Args:
        dcm (DCM): The CM controller.
    """
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
    """_unattended_run.

    Start the report running and store the run configuration in Firestore. This
    will then be monitored for completion and import by the run-monitor.

    Args:
        dcm (DCM): The CM controller.
    """
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
      self.firestore.store_document(type=Type._RUNNING,
                                    id=runner['report_id'], document=runner)
