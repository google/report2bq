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


import logging
import os
import random
import uuid

from typing import Any, Dict, List

from classes import discovery
from classes.credentials import Credentials
from classes.report_manager import ReportManager
from classes.report_type import Type
from classes.services import Service


class GA360ReportManager(ReportManager):
  report_type = Type.GA360_RPT
  actions = {
      'add',
      'delete',
      'install',
      'list',
      'show',
  }

  def manage(self, **kwargs: Dict[str, Any]) -> Any:
    project = kwargs['project']
    email = kwargs.get('email')
    self.bucket=f'{project}-report2bq-ga360-manager'
    if _api_key := kwargs.get('api_key'):
      os.environ['API_KEY'] = _api_key

    if _name := kwargs.get('name') or kwargs.get('file'):
      report_name = _name.split('/')[-1].split('.')[0]
    else:
      report_name = None

    args = {
      'report': report_name,
      'file': kwargs.get('file'),
      'project': project,
      'email': email,
      **kwargs,
    }

    return self._get_action(kwargs.get('action'))(**args)

  def install(self, project: str, email: str, file: str,
              gcs_stored: bool, **unused) -> None:
    if not self.scheduler:
      logging.warn(
        'No scheduler is available: jobs will be stored but not scheduled.')

    results = []
    random.seed(uuid.uuid4())
    runners = self._read_json(project=project, email=email,
                              file=file, gcs_stored=gcs_stored)

    for runner in runners:
      id = f'{runner["report"]}_{runner["view_id"]}'
      creds = Credentials(project=project, email=runner.get('email'))
      service = discovery.get_service(service=Service.GA360,
                                      credentials=creds)

      self.firestore.update_document(type=self.report_type,
                                     id=id, new_data=runner)

      # Now schedule.
      if self.scheduler:
        if not (description := runner.get('description')):
          if title := runner.get('title'):
            description = title
          else:
            description = (f'Runner: report {runner.get("report")}, '
                          f'view_id {runner.get("view_id")}.')
          runner['description'] = description

        runner['hour'] = runner.get('hour') or '1'
        results.append(self._schedule_job(project=project,
                                          runner=runner, id=id))
