# Copyright 2021 Google LLC
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
import unittest

from classes import csv_helpers

CSV = '''string,int,float,date,datetime,*Sales Confirm - Revenue - DDA
"Hello, my name is Inigo Montoya",0,1.00,1967-06-18,1967-06-18 06:30:00,aaa
"Hello, my name is Inigo Montoya",-1,1.20,1967-06-18,1967-06-18 06:30:00,aaa
"Hello, my name is Inigo Montoya",15,1.00,1967-06-18,1967-06-18 06:30:00,aaa
'''
HEADER = ['string', 'int', 'float', 'date', 'datetime',
          '*Sales Confirm - Revenue - DDA', ]
TYPES = [
    'STRING',
    'INTEGER',
    'FLOAT',
    'DATETIME',
    'DATETIME',
    'STRING',
]

HEADER_ONLY_CSV = '''string,int,float,date,datetime,*Sales Confirm - Revenue - DDA'''


class CSVHelpersTest(unittest.TestCase):
  def test_sanitize_string_valid(self):
    self.assertEqual('This_is_Sparta0x21',
                     csv_helpers.sanitize_title('This is Sparta!'))
    self.assertEqual('_I_Can0x27t_Get_No__Satisfaction',
                     csv_helpers.sanitize_title(
                         "(I Can't Get No) Satisfaction"))
    self.assertEqual('Jack_in_the_Green',
                     csv_helpers.sanitize_title("Jack-in-the-Green"))
    self.assertEqual('abc123ABC_',
                     csv_helpers.sanitize_title('abc123ABC_'))
    self.assertEqual('0x2aSales_Confirm___Revenue___DDA',
                     csv_helpers.sanitize_title(
                         '*Sales Confirm - Revenue - DDA'))
    self.assertEqual('X0x2aSales_Confirm___Revenue___DDA',
                     csv_helpers.sanitize_column(
                         '*Sales Confirm - Revenue - DDA'))

  def test_sanitize_string_invalid(self):
    with self.assertRaises(TypeError):
      csv_helpers.sanitize_title(None)

  def test_get_column_types(self):
    data = io.BytesIO(CSV.encode('utf-8'))
    csv_header, csv_types = csv_helpers.get_column_types(data)
    self.assertEqual(HEADER, csv_header)
    self.assertEqual(TYPES, csv_types)

  def test_create_table_schema(self):
    schema = csv_helpers.create_table_schema(HEADER, TYPES)
    self.assertEqual([
        {'name': 'string', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'int', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'float', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'date', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'X0x2aSales_Confirm___Revenue___DDA',
         'type': 'STRING',
         'mode': 'NULLABLE'},
    ], schema)

  def test_header_only_csv(self):
    data = io.BytesIO(HEADER_ONLY_CSV.encode('utf-8'))
    headers, types = csv_helpers.get_column_types(data)
    self.assertEqual(HEADER, headers)
    self.assertEqual([], types)

  def test_empty_csv(self):
    data = io.BytesIO()
    headers, types = csv_helpers.get_column_types(data)
    self.assertEqual([], headers)
    self.assertEqual([], types)
