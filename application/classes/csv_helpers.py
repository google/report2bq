# Copyright 2022 Google LLC
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

import io
import re
from typing import Dict, List, Tuple

import pandas
from pandas.errors import EmptyDataError


def get_column_types(data: io.BytesIO) -> Tuple[List[str], List[str]]:
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
  def _sql_field(T):
    R = None
    match T.dtype.name.upper():
      case 'STRING':
        R = 'STRING'
      case 'FLOAT64':
        R = 'FLOAT'
      case 'INT64':
        R = 'INTEGER'
      case _ as other if other.startswith('DATE'):
        R = 'DATETIME'
      case _:
        R = 'STRING'
        # if T.format == '%Y-%m-%d %HH:%MM:%SS':
        #   R = 'DATETIME'
        # if T.format == '%Y-%m-%d %H:%M:%S':
        #   R = 'DATETIME'
        # elif T.format == '%Y-%m-%d':
        #   R = 'DATE'

    return R or 'STRING'

  try:
    initial_df = pandas.read_csv(data)
    csv_headers = list(initial_df.columns)

    if initial_df.empty:
      csv_types = []

    else:
      for c in csv_headers:
        if initial_df[c].dtype.name == 'object':
          initial_df[c] = pandas.to_datetime(initial_df[c], errors = 'ignore')
      table_set = initial_df.convert_dtypes(infer_objects=True)
      csv_types = list([_sql_field(table_set[c]) for c in table_set.columns])

  except EmptyDataError:
    (csv_headers, csv_types) = ([], [])

  return (csv_headers, csv_types)


def sanitize_title(original: str) -> str:
  return sanitize_string(original, False)


def sanitize_column(original: str) -> str:
  return sanitize_string(original, True)


def sanitize_string(original: str, for_column: bool = False) -> str:
  """sanitize string

  Sanitize the header names (or any other string) to convert all
  non-alphanumerics to a simple '_' character. This matches what BQ expects.

  Arguments:
      original (str):  original string

  Returns:
      str: sanitized string

  re.sub('[^a-zA-Z0-9,]', '_', original)
  """
  sanitized = ''

  for char in original:
    if re.match('[a-zA-Z0-9_]', char):
      sanitized += char
    elif re.match('[ ():,-]', char):
      sanitized += '_'
    else:
      sanitized += hex(ord(char))

  if for_column and sanitized.startswith(tuple([c for c in '0123456789'])):
    sanitized = 'X' + sanitized

  return sanitized


def create_table_schema(column_headers: List[str] = None,
                        column_types: List[str] = None) \
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
    new_field['name'] = sanitize_string(col, for_column=True)
    new_field['type'] = master.get(col)
    field_list.append(new_field)

  return field_list
