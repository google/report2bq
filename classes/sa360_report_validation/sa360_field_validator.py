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

__author__ = ['davidharcombe@google.com (David Harcombe)']

from typing import Any, Dict, Tuple

from absl import app
from googleapiclient.discovery import Resource


class SA360Validator(object):
  saved_column_names = None

  def __init__(self,
               sa360_service: Resource = None,
               agency: int = None,
               advertiser: int = None) -> None:
    self.fields = []
    self.sa360_service = sa360_service
    self.agency = agency
    self.advertiser = advertiser

  def validate(self, field: Any) -> Tuple[bool, str]:
    if isinstance(field, str):
      return self.validate_custom_column(field)
    elif isinstance(field, dict):
      if 'type' not in field or field['type'] == 'savedColumnName':
        return self.validate_custom_column(field['value'])
      elif 'type' in field and field['type'] == 'columnName':
        return self.validate_standard_column(field['value'])

    return (False, None)

  def validate_custom_column(self, name: str) -> Tuple[bool, str]:
    if not name:
      return (True, '--- Blank column name ---')

    if not self.saved_column_names:
      self.list_custom_columns()

    if not self.saved_column_names:
      return (False, '--- No custom columns found --')

    if name in self.saved_column_names:
      return (True, name)

    did_you_mean = next((x for i, x in enumerate(self.saved_column_names)
                         if x.casefold() == name.casefold()), None)
    return (False, did_you_mean)

  def validate_standard_column(self, name: str) -> Tuple[bool, str]:
    if name in self.fields:
      return (True, name)

    did_you_mean = next(
      (x for i, x in enumerate(self.fields) if x.casefold() == name.casefold()),
      None)
    return (False, did_you_mean)

  def list_custom_columns(self) -> None:
    if self.sa360_service:
      request = self.sa360_service.savedColumns().list(
        agencyId=self.agency, advertiserId=self.advertiser)
      response = request.execute()

      if 'items' in response:
        self.saved_column_names = [
          item['savedColumnName'] for item in response['items']
        ]
      else:
        self.saved_column_names = []


def main(unused_argv):
  from classes.sa360_report_validation.campaign import Campaign
  c = Campaign()
  print(c.validate({'value': 'adWordsConversions', 'type': 'columnName'}))
  print(c.validate({'value': 'adWordsConversionS', 'type': 'columnName'}))
  print(c.validate({'value': 'foo', 'type': 'columnName'}))
  print(c.validate({'value': 'foo', 'type': 'savedColumnName'}))
  print(c.validate({'value': '', 'type': 'savedColumnName'}))
  print(c.validate('foo'))


if __name__ == '__main__':
  app.run(main)
