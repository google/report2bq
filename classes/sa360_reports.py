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

import collections
import json
import logging
import time

from classes import ReportRunner
from classes.report_type import Type
from classes.sa360_v2 import SA360

from contextlib import suppress
from dataclasses import dataclass
from io import StringIO
from typing import Any, Dict, List


@dataclass
class SA360ReportParameter(object):
  name: str
  path: str
  element_type: str = 'str'
  is_list: bool = False
  column_type: str = 'savedColumnName'
  ordinal: int = None       # Now deprecated


class SA360ReportTemplate(object):
  def _update(self, field: SA360ReportParameter, original: Dict[str, Any], new: Dict[str, Any]):
    for key, val in new.items():
      if isinstance(val, collections.Mapping):
        tmp = self._update(field, original.get(key, { }), val)
        original[key] = tmp
      elif isinstance(val, list):
        ordinal = 0
        for _item in original[key]:
          if [ value for value in _item.values() if value == field.name ]:
            for k in _item.keys():
              if _item[k] == field.name:
                original[key][ordinal] = { field.column_type: val[0] }
          ordinal += 1
      else:
        original[key] = new[key]

    return original


  def _insert(self, data: Dict[Any, Any], field: SA360ReportParameter, value: Any):
    _path_elements = field.path.split('.')
    _path_elements.reverse()

    _data = None

    if field.element_type == 'int':
      _value = int(value)
    else:
      _value = value

    try:
      for _element in _path_elements:
        if not _data:
          if field.is_list:
            _data = { _element: [_value] }
          else:
            _data = { _element: _value }
        else:
          _data = {_element: _data }

    except KeyError as k:
      logging.info(f'Error replacing {self.path}{("["+self.ordinal+"]") if self.ordinal else ""} - not found in data.')

    return _data


  def prepare(self, template: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    _parameters = template['parameters']
    _report = template['report']

    for _parameter in _parameters:
      _param = SA360ReportParameter(**_parameter)
      with suppress(KeyError):
        value = values[_param.name]
        _new = self._insert(data=_report, field=_param, value=value)
        _report = self._update(field=_param, original=_report, new=_new)

    # Filter out blank column names
    _columns = list(filter(lambda n: n.get('columnName', n.get('savedColumnName', '')) != '', _report['columns']))
    _report['columns'] = _columns
    return _report