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
from classes import report_manager
from classes.credentials import Credentials
from classes.report_type import Type
from classes.services import Service


class GA360ReportManager(report_manager.ReportManager):
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
    self.bucket = f'{project}-report2bq-ga360-manager'
    if _api_key := kwargs.get('api_key'):
      os.environ['API_KEY'] = _api_key

    if _name := kwargs.get('name') or kwargs.get('file'):
      report_name = _name.split('/')[-1].split('.')[0]
    else:
      report_name = None

    source = None
    if kwargs.get('file') and kwargs.get('gcs_stored'):
      source = report_manager.ManagerType.FILE_GCS
    elif kwargs.get('file'):
      source = report_manager.ManagerType.FILE_LOCAL
    else:
      source = report_manager.ManagerType.BIG_QUERY

    config: report_manager.ManagerConfiguration = \
        report_manager.ManagerConfiguration(
            type=source,
            project=project,
            email=email,
            file=kwargs.get('file'),
            table='sa360_manager_input'
        )

    args = {
        'report': report_name,
        'config': config,
        **kwargs,
    }

    return self._get_action(kwargs.get('action'))(**args)

  def install(self,
              config: report_manager.ManagerConfiguration, **unused) -> None:
    if not self.scheduler:
      logging.warn(
          'No scheduler is available: jobs will be stored but not scheduled.')

    results = []
    random.seed(uuid.uuid4())
    runners = self._read_json(config)

    for runner in runners:
      id = f'{runner["report"]}_{runner["view_id"]}'
      creds = Credentials(project=config.project, email=runner.get('email'))
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
        results.append(self._schedule_job(project=config.project,
                                          runner=runner, id=id))
