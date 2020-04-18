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
import logging
import re

from messytables import CSVTableSet
from messytables.types import StringType, IntegerType, FloatType, \
        DecimalType, DateType, DateUtilType, BoolType
from messytables import type_guess
from messytables import types_processor
from messytables import headers_guess
from messytables import headers_processor
from messytables import offset_processor
from typing import Dict, List

class CSVHelpers(object):
  """CSV file helpers  
  
  """
  @staticmethod
  def get_column_types(data: io.BytesIO) -> (List[str], List[str]):
    """derive the column types

    Using messytables' CSV API, attempt to derive the column types based on a best-guess
    of a sample of the rows.

    This is still a WIP due to the parlous state of the DV360/CM CSV data formats in
    general
    
    Arguments:
        data {io.BytesIO} -- sample of the CSV file

    Returns:
        (List[str], List[str]) -- tuple of list of header names and list of column types
    """
    table_set = CSVTableSet(data)
    row_set = table_set.tables[0]
    offset, headers = headers_guess(row_set.sample)
    logging.info(headers)
    row_set.register_processor(headers_processor(headers))
    row_set.register_processor(offset_processor(offset + 1))
    types = type_guess(row_set.sample, strict=True)
    logging.info(types)

    return (headers, types)


  @staticmethod
  def sanitize_string(original: str) -> str:
    """sanitize string

    Sanitize the header names (or any other string) to convert all non-alphanumerics to
    a simple '_' character. This matches what BQ expects.
    
    Arguments:
        original {str} -- original string
    
    Returns:
        str -- sanitized string
    """
    return re.sub('[^a-zA-Z0-9,]', '_', original)


  @staticmethod
  def create_table_schema(header: List[str]=None, types: List[str]=None) -> Dict[str, str]:
    """create big query table schema

    Takes the list of column names and produces a json format Big Query schema suitable
    for defining the import CSV.

    TODO: Also accept the column types and create the schema that way
    
    Keyword Arguments:
        header {list} -- header column names (default: {None})
    
    Returns:
        Dict[str, str] -- json format schema
    """
    def _sql_field(T):
      if isinstance(T, StringType): R = 'STRING'
      elif isinstance(T, DecimalType): R = 'FLOAT64'
      elif isinstance(T, IntegerType): R = 'INT64'
      elif isinstance(T, DateType): 
        if T.format == '%Y-%m-%d %HH:%MM:%SS': R = 'DATETIME'
        elif T.format == '%Y-%m-%d': R = 'DATE'
        else: R = 'STRING'
      else: R = 'STRING'

      return R

    field_template = {
        "name": "",
        "type": "STRING",
        "mode": "NULLABLE"
    }
    field_list = []

    master = dict(zip(header, types or (['STRING'] * len(header))))

    # cols = re.sub('[^a-zA-Z0-9,]', '_', header).split(',')
    for col in header:
      new_field = field_template.copy()
      new_field['name'] = CSVHelpers.sanitize_string(col)
      new_field['type'] = _sql_field(master.get(col))
      field_list.append(new_field)

    return field_list
