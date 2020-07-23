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

from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from typing import Any, Dict, Mapping

from classes.decorators import retry
from classes.report_type import Type

class Fetcher(object):
  @retry(exceptions=HttpError, tries=3, backoff=2)
  def fetch(self, method, **kwargs: Mapping[str, str]) -> Dict[str, Any]:
    result = method(**kwargs).execute()
    return result


class ReportFetcher(object):
  report_type  = None

  def read_header(self, report_details: dict) -> list: pass

  def stream_to_gcs(self, bucket: str, report_details: Dict[str, Any]) -> None: pass

  def normalize_report_details(self, report_object: Dict[str, Any], report_id: str): pass

  def get_latest_report_file(self, report_id: str): pass

  def run_report(self, report_id: int): pass

  def check_running_report(self, config: Dict[str, Any]): pass

  def get_reports(self) -> Dict[str, Any]: pass

  def service(self) -> Resource: pass


class ReportRunner(object):
  report_type = None

  def run(self, unattended: bool): pass
