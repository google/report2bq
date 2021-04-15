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
import enum
import re

import dataclasses_json
from dataclasses_json.undefined import Undefined

from classes import decorators

from datetime import date
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from typing import Any, Dict, List, Optional


class GA360MetricType(enum.Enum):
  METRIC_TYPE_UNSPECIFIED = 'METRIC_TYPE_UNSPECIFIED'
  INTEGER = 'INTEGER'
  FLOAT = 'FLOAT'
  CURRENCY = 'CURRENCY'
  PERCENT = 'PERCENT'
  TIME = 'TIME'

  @classmethod
  def _missing_(cls, value):
    """Fix bad metric types

    Args:
        value (str): enum string value requested

    Returns:
        METRIC_TYPE_UNSPECIFIED: if invalid
    """
    return cls.METRIC_TYPE_UNSPECIFIED


class GA360SamplingLevel(enum.Enum):
  DEFAULT = 'DEFAULT'
  SMALL = 'SMALL'
  LARGE = 'LARGE'

  @classmethod
  def _missing_(cls, value):
    """Fix bad types

    Args:
        value (str): enum string value requested

    Returns:
        DEFAULT: if invalid
    """
    return cls.DEFAULT


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL,
                                 undefined=Undefined.EXCLUDE)
@dataclasses.dataclass
class GA360ReportMetric(object):
  expression: str
  alias: Optional[str] = None
  formatting_type: Optional[GA360MetricType] = None

  @property
  def metric(self) -> Dict[str, str]:
    metric = { 'expression': self.expression }
    if self.alias:
      metric['alias'] = self.alias
    if self.formatting_type:
      metric['formattingType'] = GA360MetricType(self.formatting_type).value

    return metric


PATTERN = re.compile('(^[0-9]+)([a-z]+s)Ago', re.IGNORECASE)


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL,
                                 undefined=Undefined.EXCLUDE)
@dataclasses.dataclass
class GA360DateRange(object):
  start_date: Optional[str] = None
  end_date: Optional[str] = None

  def _parse_date(self, d: str) -> str:
    try:
      _d = date.fromisoformat(d)

    except ValueError:
      if d.casefold() == 'today':
        o = { 'days': 0 }
      elif d.casefold() == 'yesterday':
        o = { 'days': -1 }
      elif match := re.match(PATTERN, d):
          if not (
            lambda m: m in [
              'days', 'weeks', 'months', 'years'])(match.group(2).lower()):
            raise NotImplementedError(f'Unknown date offset type: {d}')
          o = { match.group(2).lower(): -1 * int(match.group(1)) }
      else:
        raise NotImplementedError(f'Unknown date offset type: {d}')

      _d = self._now + relativedelta(**o)

    return _d.isoformat()

  @decorators.lazy_property
  def _now(self) -> date:
    return date.today()

  @property
  def date_range(self) -> Dict[str, str]:
    dateRange = {}
    dateRange['startDate'] = \
      self._parse_date(self.start_date) \
        if self.start_date else self._parse_date('7daysAgo')
    dateRange['endDate'] = \
      self._parse_date(self.end_date) \
        if self.end_date else self._parse_date('yesterday')
    return dateRange


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL,
                                 undefined=Undefined.EXCLUDE)
@dataclasses.dataclass
class GA360ReportDefinition(object):
  view_id: str
  metrics: List[GA360ReportMetric]
  dimensions: List[str] = \
    dataclasses.field(metadata=dataclasses_json.config(
    decoder=lambda dimensions: [ d.get('name') for d in dimensions ]))
  date_ranges: Optional[List[GA360DateRange]] = None
  page_size: Optional[str] = None
  page_token: Optional[str] = None
  sampling_level: Optional[GA360SamplingLevel] = None

  @property
  def report_request(self) -> Dict[str, str]:
    report_request = {
      'viewId': self.view_id,
      'dimensions': [ { 'name': d } for d in self.dimensions ],
      'metrics': [  m.metric for m in self.metrics ],
    }

    if self.sampling_level:
      report_request['samplingLevel'] = self.sampling_level.value

    if self.page_size:
      report_request['pageSize'] = self.page_size

    if self.page_token:
      report_request['pageToken'] = self.page_token

    if self.date_ranges:
      if (num := len(self.date_ranges)) > 2:
        raise ValueError(
          'GA360 report error: Max 2 date ranges, %d supplied', num)
      report_request['dateRanges'] = [ r.date_range for r in self.date_ranges ]
    else:
      report_request['dateRanges'] = [ GA360DateRange().date_range ]

    return report_request
