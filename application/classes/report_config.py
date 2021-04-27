# Copyright 2021 Google LLC
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

import dataclasses
import dataclasses_json
import enum
import json

from datetime import date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, List, Optional



class Partitioning(enum.Enum):
  NONE = None
  INGESTION = 'ingestion'
  INFER = 'infer'

  def __repr__(self) -> str:
      return self.name

  @classmethod
  def _missing_(cls, value):
    """Fix bad metric types

    Args:
        value (str): enum string value requested

    Returns:
        METRIC_TYPE_UNSPECIFIED: if invalid
    """
    return cls.NONE


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Notifier(object):
  message: str

@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Schema(object):
  name: str
  type: str
  mode: str

@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Urls(object):
  apiUrl: Optional[str] = None
  browserUrl: Optional[str] = None

@dataclasses_json.dataclass_json
@dataclasses.dataclass
class DateRange(object):
  kind: Optional[str] = None
  startDate: Optional[str] = None
  endDate: Optional[str] = None

@dataclasses_json.dataclass_json
@dataclasses.dataclass
class File(object):
  id: Optional[str] = None
  reportId: Optional[str] = None
  etag: Optional[str] = None
  fileName: Optional[str] = None
  format: Optional[str] = None
  kind: Optional[str] = None
  lastModifiedTime: Optional[str] = None
  status: Optional[str] = None
  reportName: Optional[str] = None
  urls: Optional[Urls] = None
  dateRange: Optional[DateRange] = None

@dataclasses_json.dataclass_json
@dataclasses.dataclass
class ReportConfig(object):
  id: str
  email: Optional[str] = None
  name: Optional[str] = None
  report_name: Optional[str] = None
  type: Optional[str] = None
  update_cadence: Optional[str] = None
  profile_id: Optional[str] = None
  current_path: Optional[str] = None
  dest_project: Optional[str] = None
  dest_dataset: Optional[str] = None
  dest_table: Optional[str] = None
  last_updated: Optional[str] = None
  partition: Optional[Partitioning] = dataclasses.field(
    metadata=dataclasses_json.config(
      encoder= lambda p: None if p == Partitioning.NONE else p.value
    ),
    default=Partitioning.NONE
  )
  partition_column: Optional[str] = None
  append: Optional[bool] = False
  table_name: Optional[str] = None
  url: Optional[str] = None
  infer_schema: Optional[bool] = False
  notifier: Optional[Notifier] = None
  force: Optional[bool] = False
  drop_table: Optional[bool] = False
  development: Optional[bool] = False

  report_file: Optional[File] = None
  schema: Optional[List[Schema]] = None
