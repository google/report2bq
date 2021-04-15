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
import unittest

from classes import firestore
from classes.ga360_report_manager import GA360ReportManager
from classes.report_type import Type
from google.cloud import firestore as gfs
from unittest import mock


class GA360ReportManagerTest(unittest.TestCase):

  def setUp(self):
    self.mock_firestore_client = mock.create_autospec(gfs.Client)

  def test_type(self):
    self.assertEqual(Type.GA360_RPT, GA360ReportManager().report_type)

  def test_missing_parameter(self):
    with self.assertRaises(KeyError):
      GA360ReportManager().manage(**{'action': 'list'})

  def test_valid(self):
    with mock.patch.object(firestore.Firestore, 'client'):
      r = GA360ReportManager().manage(
        **{
            'action': 'list',
            'project': 'foo',
            'file': 'foo.csv',
            'gcs_stored': True
          })

  def test_invalid_super_action(self):
    with self.assertRaisesRegex(NotImplementedError, 'Not implemented'):
      with mock.patch.object(firestore.Firestore, 'client'):
        manager = GA360ReportManager()
        manager.actions = {'validate'}
        r = manager.manage(
          **{
              'action': 'validate',
              'project': 'foo',
              'file': 'foo.csv',
              'gcs_stored': True
            })

  def test_completely_invalid_action(self):
    with self.assertRaisesRegex(NotImplementedError, 'Action "wibble".*'):
      with mock.patch.object(firestore.Firestore, 'client'):
        r = GA360ReportManager().manage(
          **{
              'action': 'wibble',
              'project': 'foo',
              'file': 'foo.csv',
              'gcs_stored': True
            })
