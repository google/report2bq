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


import os

from typing import Any, Dict, List

from classes.report_manager import ReportManager
from classes.report_type import Type


class GA360ReportManager(ReportManager):
  report_type = Type.GA360_RPT
  actions = {
      'list',
      'show',
      'add',
      'delete',
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
