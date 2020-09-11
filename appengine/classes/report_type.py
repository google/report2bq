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

from enum import Enum

class Type(Enum):
  ADH = 'adh'
  CM = 'cm'
  DV360 = 'dv360'
  SA360 = 'sa360'
  SA360_RPT = 'sa360_report'

  # Internal use only
  _JOBS = 'jobs'
  _RUNNING = 'running'
  _ADMIN = 'administration'
  
  def runner(self, report_id: str):
    return {
      Type._JOBS: None,
      Type._RUNNING: None,
      Type._ADMIN: None,
    }.get(self, f'run-{self.value}-{report_id}')

  def __str__(self):
    return str(self.value)