# Copyright 2022 Google LLC
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

import immutabledict
import enum


class Type(enum.Enum):
  ADH = 'adh'
  CM = 'cm'
  DV360 = 'dv360'
  GA360_RPT = 'ga360_report'
  SA360 = 'sa360'
  SA360_RPT = 'sa360_report'

  # Internal use only
  _ADMIN = 'administration'
  _COMPLETED = 'jobs-completed'
  _JOBS = 'jobs'
  _RUNNING = 'running'

  # Missing value
  _UNKNOWN = 'unknown'

  @classmethod
  def _missing_(cls, value) -> Type:
    """Backward compatilbility for old enums.

    If the old product names are still in use in some Firestore or other confguration
    systems (this is possible post migration from the older versions), replace them with
    the new values seamlessly.

    Args:
        value (str): enum string value requested

    Returns:
        Type: the corrected type

    Raises:
        ValueError if it was simply an incorrect enum rather than dv360/dbm or dcm/cm confusion
    """
    if value == 'dbm':
      return cls.DV360
    elif value == 'dcm':
      return cls.CM
    elif value == 'ga360':
      return cls.GA360_RPT
    else:
      return cls._UNKNOWN

  @property
  def api_name(self) -> str:
    return API_NAMES.get(self)

  def runner(self, report_id: str) -> str:
    return None if self.name.startswith('_') \
        else f'run-{self.value}-{report_id}'

  def fetcher(self, report_id: str) -> str:
    return None if self.name.startswith('_') \
        else f'fetch-{self.value}-{report_id}'

  def __str__(self) -> str:
    return str(self.value)

  def __repr__(self) -> str:
    return str(self.value)


API_NAMES: immutabledict.immutabledict = immutabledict.immutabledict({
    Type.ADH: 'adsdatahub',
    Type.CM: 'dfareporting',
    Type.DV360: 'doubleclickbidmanager',
    Type.GA360_RPT: 'analyticsreporting',
    Type.SA360: 'doubleclicksearch',
    Type.SA360_RPT: 'doubleclicksearch',
})
