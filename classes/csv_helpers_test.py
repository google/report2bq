import io
import messytables
import unittest

from classes import csv_helpers

from messytables import types

CSV = '''string,int,float,date,datetime,string
"Hello, my name is Inigo Montoya",0,1.00,1967-06-18,1967-06-18 06:30:00,aaa
"Hello, my name is Inigo Montoya",-1,1.20,1967-06-18,1967-06-18 06:30:00,aaa
"Hello, my name is Inigo Montoya",15,1.00,1967-06-18,1967-06-18 06:30:00,aaa
'''
HEADER = ['string', 'int', 'float', 'date', 'datetime', 'string',]
TYPES = [
  types.StringType(),
  types.IntegerType(),
  types.DecimalType(),
  types.DateType('%Y-%m-%d'),
  types.DateType('%Y-%m-%d %H:%M:%S'),
  types.StringType(),
]

HEADER_ONLY_CSV = '''string,int,float,date,datetime,string'''


class CSVHelpersTest(unittest.TestCase):
  def test_sanitize_string_valid(self):
    self.assertEqual('This_is_Sparta_',
                     csv_helpers.sanitize_string('This is Sparta!'))
    self.assertEqual('_I_Can_t_Get_No__Satisfaction',
                     csv_helpers.sanitize_string(
                       "(I Can't Get No) Satisfaction"))

  def test_sanitize_string_invalid(self):
    with self.assertRaises(TypeError):
      csv_helpers.sanitize_string(None)

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
      {'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'},
      {'name': 'datetime', 'type': 'DATETIME', 'mode': 'NULLABLE'},
      {'name': 'string', 'type': 'STRING', 'mode': 'NULLABLE'},
    ], schema)

  def test_header_only_csv(self):
    data = io.BytesIO(HEADER_ONLY_CSV.encode('utf-8'))
    headers, types = csv_helpers.get_column_types(data)
    self.assertEqual(HEADER, headers)
    self.assertEqual([], types)

  def test_empty_csv(self):
    data = io.BytesIO()
    _headers, _types = csv_helpers.get_column_types(data)
    self.assertEqual([], _headers)
    self.assertEqual([], _types)
