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

from classes import ReportFetcher
from classes.dbm import DBM
from classes.dcm import DCM
from classes.sa360_web import SA360Web
from classes.sa360_dynamic import SA360Dynamic
from classes.report_type import Type


def create_fetcher(product: Type, **kwargs) -> ReportFetcher:
  if fetcher := {
    Type.DV360: DBM,
    Type.CM: DCM,
    Type.SA360: SA360Web,
    Type.SA360_RPT: SA360Dynamic,
  }.get(product):
    return fetcher(**kwargs)

  raise Exception(f'Cannot create fetcher for {product}')
