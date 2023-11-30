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

from typing import Any, Dict, List, Tuple

from classes.decorators import lazy_property
from googleapiclient.discovery import Resource
from typing import List
import logging as log
from google.cloud import logging
from classes import gmail

logging_client = logging.Client()
logging_client.setup_logging()


class SA360Validator(object):
  fields = []

  def __init__(self,
               sa360_service: Resource = None,
               agency: int = None,
               advertiser: int = None) -> None:
    self.sa360_service = sa360_service
    self.agency = agency
    self.advertiser = advertiser

  @lazy_property
  def saved_column_names(self) -> List[str]:
    return self.list_custom_columns()

  def validate(self, field: Any) -> Tuple[bool, str]:
    if isinstance(field, str):
      return self.validate_custom_column(field)

    elif isinstance(field, dict):
      field_type = field.get('type')
      if field_type == 'savedColumnName':
        return self.validate_custom_column(field['value'])
      elif field_type == 'columnName':
        return self.validate_standard_column(field['value'])
      else:
        # 'type' not specified. rather than fail, check both in order
        (valid, name) = self.validate_custom_column(field['value'])
        if valid:
          field['type'] = 'savedColumnName'
          return (valid, name)
        else:
          field['type'] = 'columnName'
          return self.validate_standard_column(field['value'])

  def validate_custom_column(self, name: str) -> Tuple[bool, str]:
    if not name:
      return (True, '--- Blank column name ---')

    if not self.saved_column_names:
      return (False, '--- No custom columns found ---')

    if name in self.saved_column_names:
      return (True, name)

    return (False, self._find_bad_case(name, self.saved_column_names))

  def validate_standard_column(self, name: str) -> Tuple[bool, str]:
    if not name:
      return (True, '--- Blank column name ---')

    if name in self.fields:
      return (True, name)

    return (False, self._find_bad_case(name, self.fields))

  def list_custom_columns(self) -> List[str]:
    saved_column_names = []
    try:
      if self.sa360_service:
        request = self.sa360_service.savedColumns().list(
            agencyId=self.agency, advertiserId=self.advertiser)
        response = request.execute()

        if 'items' in response:
          saved_column_names = [
              item['savedColumnName'] for item in response['items']
          ]
    except Exception as e:
      log.info(gmail.error_to_trace(e))

    return saved_column_names

  def _find_bad_case(self, name: str, columns: List[str]) -> str:
    return next((x for i, x in enumerate(columns)
                 if x.casefold() == name.casefold()), None)
