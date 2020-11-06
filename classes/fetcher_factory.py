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

from classes import ReportFetcher
from classes.dbm import DBM
from classes.dcm import DCM
from classes.sa360_web import SA360Web
from classes.sa360_dynamic import SA360Dynamic
from classes.report_type import Type


class FetcherFactory(object):
  @staticmethod
  def create_fetcher(product: Type, **kwargs) -> ReportFetcher:
    fetcher = None

    if product == Type.DV360:
      fetcher = DBM(**kwargs)

    elif product == Type.CM:
      fetcher = DCM(**kwargs)

    elif product == Type.SA360:
      fetcher = SA360Web(**kwargs)

    elif product == Type.SA360_RPT:
      fetcher = SA360Dynamic(**kwargs)

    else:
      raise Exception(f'Cannot create fetcher for {product}')

    return fetcher