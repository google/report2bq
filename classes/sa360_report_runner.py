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
import pprint
import pytz
import time

from classes import ReportRunner
from classes.discovery import DiscoverService
from classes.firestore import Firestore
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.sa360_reports import SA360ReportParameter, SA360ReportTemplate
from classes.sa360_v2 import SA360
from classes.services import Service

from dataclasses import dataclass
from datetime import datetime, timedelta
from dateutil.parser import parse
from enum import Enum, auto
from io import StringIO
from typing import Any, Dict, List


class SA360ReportRunner(ReportRunner):
  report_type = Type.SA360_RPT

  def __init__(self, report_id: str, email: str, project: str=None, timezone: str=None):
    self.email = email
    self.report_id = report_id
    self.project = project
    self.timezone = timezone

    self.firestore = Firestore()


  def run(self, unattended: bool = True) -> Dict[str, Any]:
    # TODO: Make SA360 object here
    sa360 = SA360(self.email, self.project)

    if unattended:
      return self._unattended_run(sa360=sa360)
    else:
      return self._attended_run(sa360=sa360)


  def _unattended_run(self, sa360: SA360) -> Dict[str, Any]:
    report_config = self.firestore.get_document(type=Type.SA360_RPT, id=self.report_id)
    if not report_config:
      raise NotImplementedError(f'No such runner: {self.report_id}')

    _tz = pytz.timezone(report_config.get('timezone') or self.timezone or 'America/Toronto')
    _today = datetime.now(_tz)

    report_config['StartDate'] = (_today - timedelta(days=(report_config.get('offset') or 0))).strftime('%Y-%m-%d')
    report_config['EndDate'] = (_today - timedelta(days=(report_config.get('lookback') or 0))).strftime('%Y-%m-%d')

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
    return runner


  def _attended_run(self, sa360: SA360) -> None: 
    raise NotImplementedError()
