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

import io
import messytables
import re

from messytables import types
from typing import Dict, List, Tuple


def get_column_types(data: io.BytesIO) \
  -> Tuple[List[str], List[types.CellType]]:
  """derive the column types

  Using messytables' CSV API, attempt to derive the column types based on a
  best-guess of a sample of the rows.

  This is still a WIP due to the parlous state of the DV360/CM CSV data formats
  in general

  Arguments:
      data (io.BytesIO):  sample of the CSV file

  Returns:
      (List[str], List[str]): tuple of list of header names and list of
                                column types
  """
  table_set = messytables.CSVTableSet(data)
  row_set = table_set.tables[0]
  offset, csv_headers = messytables.headers_guess(row_set.sample)
  row_set.register_processor(messytables.headers_processor(csv_headers))
  row_set.register_processor(messytables.offset_processor(offset + 1))
  csv_types = messytables.type_guess(row_set.sample, strict=True)

  return (csv_headers, csv_types)

def sanitize_string(original: str) -> str:
  """sanitize string

  Sanitize the header names (or any other string) to convert all
  non-alphanumerics to a simple '_' character. This matches what BQ expects.

  Arguments:
      original (str):  original string

  Returns:
      str: sanitized string

  re.sub('[^a-zA-Z0-9,]', '_', original)
  """
  replacement = ''

  for char in original:
    if re.match('[a-zA-Z0-9_]', char): replacement += char
    elif re.match('[ ():,-]', char): replacement += '_'
    else: replacement += hex(ord(char))

  return replacement

def create_table_schema(column_headers: List[str] = None,
                        column_types: List[types.CellType] = None) \
  -> List[Dict[str, str]]:
  """create big query table schema

  Takes the list of column names and produces a json format Big Query schema
  suitable for defining the import CSV.

  TODO: Also accept the column types and create the schema that way

  Keyword Arguments:
      column_headers (list):  header column names (default: {None})
      column_types (list):  column types (default: {None})

  Returns:
      List[Dict[str, str]]: json format schema
  """
  def _sql_field(T):
    R = None
    if isinstance(T, types.StringType):
      R = 'STRING'
    elif isinstance(T, types.DecimalType):
      R = 'FLOAT'
    elif isinstance(T, types.IntegerType):
      R = 'INTEGER'
    elif isinstance(T, types.DateType):
      if T.format == '%Y-%m-%d %HH:%MM:%SS':
        R = 'DATETIME'
      if T.format == '%Y-%m-%d %H:%M:%S':
        R = 'DATETIME'
      elif T.format == '%Y-%m-%d':
        R = 'DATE'

    return R or 'STRING'

  field_template = {
      "name": "",
      "type": "STRING",
      "mode": "NULLABLE"
  }
  field_list = []

  master = dict(zip(column_headers, column_types or (
      ['STRING'] * len(column_headers))))

  for col in column_headers:
    new_field = field_template.copy()
    new_field['name'] = sanitize_string(col)
    new_field['type'] = _sql_field(master.get(col))
    field_list.append(new_field)

  return field_list
