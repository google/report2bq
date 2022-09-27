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
from datetime import datetime, timedelta
from typing import Any, Dict

import pytz
from service_framework import service_builder

from classes import ReportRunner
from classes.report_type import Type
from classes.sa360_dynamic import SA360Dynamic
from classes.sa360_reports import SA360ReportTemplate


class SA360ReportRunner(ReportRunner):
  """SA360ReportRunner.

  Run SA360 Dynamic reports on demand.
  """
  report_type = Type.SA360_RPT

  def __init__(self, report_id: str, email: str, project: str = None,
               timezone: str = None, **unused) -> SA360ReportRunner:
    """Initialize the runner.

    The runner inherits from ReportRunner, which mandates the 'run' method.

    Args:
        report_id (str, optional): SA360 report id. Defaults to None.
        email (str, optional): User email for the token. Defaults to None.
        project (str, optional): Project. Defaults to None but should be
          pre-populated with the current project by the caller.
        timezone (str, optional): Timezone for the report. This is a standard TZ
          string, withe default resulting in the report running assuming UTC.

    Returns:
        DCMReportRunner: self
    """
    self.email = email
    self.project = project
    self.report_id = report_id
    self.timezone = timezone

  def run(self, unattended: bool = True) -> Dict[str, Any]:
    """Perform the report run

    Args:
        unattended (bool, optional): Is this a fire and forget (True) or wait
          for the report to complete (False). Defaults to True.
    """
    sa360 = SA360Dynamic(self.email, self.project)

    if unattended:
      return self._unattended_run(sa360=sa360)
    else:
      return self._attended_run()

  def _unattended_run(self, sa360: SA360Dynamic) -> Dict[str, Any]:
    """_unattended_run.

    Start the report running and store the run configuration in Firestore. This
    will then be monitored for completion and import by the run-monitor.

    Args:
        sa360 (SA360Dynamic): The SA360 controller.
    """
    runner = None
    report_config = None
    try:

      report_config = \
          self.firestore.get_document(type=Type.SA360_RPT, id=self.report_id)
      if not report_config:
        raise NotImplementedError(f'No such runner: {self.report_id}')

      _tz = \
          pytz.timezone(report_config.get('timezone') or
                        self.timezone or 'America/Toronto')
      _today = datetime.now(_tz)

      report_config['StartDate'] = \
          (_today - timedelta(
              days=(report_config.get('offset', 0)))).strftime('%Y-%m-%d')
      report_config['EndDate'] = \
          (_today - timedelta(
              days=(report_config.get('lookback', 0)))).strftime('%Y-%m-%d')

      template = \
          self.firestore.get_document(Type.SA360_RPT,
                                      '_reports').get(report_config['report'])
      request_body = \
          SA360ReportTemplate().prepare(template=template, values=report_config)
      sa360_service = \
          service_builder.build_service(service=self.report_type.service,
                                        key=sa360.creds.credentials)
      request = sa360_service.reports().request(body=request_body)
      response = request.execute()
      logging.info(response)

      runner = {
          'type': Type.SA360_RPT.value,
          'project': self.project,
          'report_id': self.report_id,
          'email': self.email,
          'file_id': response['id']
      }
      self.firestore.store_document(type=Type._RUNNING,
                                    id=runner['report_id'], document=runner)

    except Exception as e:
      self._email_error(email=self.email, error=e, report_config=report_config,
                        message=f'Error in SA360 Report Runner for report {self.report_id}\n\n')

    finally:
      return runner

  def _attended_run(self) -> None:
    """_attended_run.

    Not implemented for SA360Dynamic.

    Raises:
        NotImplementedError
    """
    raise NotImplementedError('Unavailable for SA360 Dynamic reports.')
