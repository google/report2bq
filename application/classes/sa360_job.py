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
from typing import Optional, Union


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class SA360ReportMetric(object):
  value: str
  type: str


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class SA360ReportNotifier(object):
  topic: str
  message: str


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class SA360Job(object):
  report: str
  email: str
  AgencyId: str
  AdvertiserId: str
  ConversionMetric: Union[SA360ReportMetric, str]
  RevenueMetric: Union[SA360ReportMetric, str]
  minute: str
  timezone: Optional[str] = None
  dest_dataset: Optional[str] = 'report2bq'
  agencyName: Optional[str] = None
  advertiserName: Optional[str] = None
  offset: int = 0
  lookback: int = 0
  notifier: Optional[SA360ReportNotifier] = None
  description: Optional[str] = None
