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

import collections
import json
import logging
import os
import pprint
import pytz
import time

from classes import ReportRunner
from classes.credentials import Credentials
from classes.discovery import DiscoverService
from classes.firestore import Firestore
from classes.gmail import GMail, GMailMessage
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.sa360_reports import SA360ReportParameter, SA360ReportTemplate
from classes.sa360_dynamic import SA360Dynamic
from classes.services import Service

from dataclasses import dataclass
from datetime import datetime, timedelta
from dateutil.parser import parse
from enum import Enum, auto
from io import StringIO
from typing import Any, Dict, List


class SA360ReportRunner(ReportRunner):
  report_type = Type.SA360_RPT

  def __init__(
    self, report_id: str, email: str, project: str=None, timezone: str=None,
    **unused):
    self.email = email
    self.report_id = report_id
    self.project = project
    self.timezone = timezone

    self.firestore = Firestore()


  def run(self, unattended: bool = True) -> Dict[str, Any]:
    # TODO: Make SA360 object here
    sa360 = SA360Dynamic(self.email, self.project)

    if unattended:
      return self._unattended_run(sa360=sa360)
    else:
      return self._attended_run(sa360=sa360)


  def _unattended_run(self, sa360: SA360Dynamic) -> Dict[str, Any]:
    runner = None
    report_config = None
    try:

      report_config = self.firestore.get_document(type=Type.SA360_RPT, id=self.report_id)
      if not report_config:
        raise NotImplementedError(f'No such runner: {self.report_id}')

      _tz = pytz.timezone(report_config.get('timezone') or self.timezone or 'America/Toronto')
      _today = datetime.now(_tz)

      report_config['StartDate'] = \
        (_today - timedelta(
          days=(report_config.get('offset', 0)))).strftime('%Y-%m-%d')
      report_config['EndDate'] = \
        (_today - timedelta(
          days=(report_config.get('lookback', 0)))).strftime('%Y-%m-%d')

      template = self.firestore.get_document(Type.SA360_RPT, '_reports').get(report_config['report'])
      request_body = SA360ReportTemplate().prepare(template=template, values=report_config)
      sa360_service = DiscoverService.get_service(Service.SA360, sa360.creds)
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
      self.firestore.store_report_runner(runner)

    except Exception as e:
      self._email_error(email=self.email, error=e, report_config=report_config,
        message=f'Error in SA360 Report Runner for report {self.report_id}')

    finally:
      return runner

  def _email_error(self, message: str, email: str=None,
    error: Exception=None, report_config: Dict[str, Any]=None) -> None:
    _to = [email] if email else []
    _administrator = os.environ.get('ADMINISTRATOR_EMAIL') or self.FIRESTORE.get_document(Type._ADMIN, 'admin').get('email')
    _cc = [_administrator] if _administrator else []

    if _to or _cc:
      message = GMailMessage(
        to=_to,
        cc=_cc,
        subject=message,
        body=f'''
{message}

Config: {report_config if report_config else 'Config unknown.'}

Error: {error if error else 'No exception.'}
''',
        project=os.environ.get('GCP_PROJECT'))

      GMail().send_message(
        message=message,
        credentials=Credentials(email=email, project=os.environ.get('GCP_PROJECT'))
      )

  def _attended_run(self, sa360: SA360Dynamic) -> None:
    raise NotImplementedError()
