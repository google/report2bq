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

import logging
import unittest

from classes import firestore, scheduler
from classes.report_manager import ReportManager
from classes.report_type import Type
from unittest import mock


STANDARD_ARGS = {
  'gcs_stored': True,
  'email': 'luke@skywalker.com',
}

REPORT_CONFIG = {
  "viewId": "",
  "dateRanges": [{"startDate": "", "endDate": ""}],
  "metrics": [{"expression": "ga:goal13Starts","alias": "Goal 13 Starts"},
              {"expression": "ga:dbmCost", "alias": "DV360 Cost"},
              {"expression": "ga:dcmCost", "alias": "CM360 Cost"}],
  "dimensions": [{"name": "ga:dimension6"},
                 {"name": "ga:dcmLastEventAdvertiser"},
                 {"name": "ga:dcmLastEventSitePlacement"},
                 {"name": "ga:dcmLastEventSitePlacementId"}]
}

class ReportManagerTest(unittest.TestCase):
  maxDiff = None

  class _Manager(ReportManager):
    type = Type.GA360_RPT
    bucket = 'manager-bucket'


  def setUp(self):
    self.mock_firestore = mock.create_autospec(firestore.Firestore)

  def test_list_valid(self):
    manager = ReportManagerTest._Manager()
    manager.type = Type.GA360_RPT
    manager._output_results = mock.Mock()
    manager._lazy_firestore = self.mock_firestore

    self.mock_firestore.list_documents.side_effect = [
      ['foo_0_0', 'foo_0_1'],
      ['foo']
    ]
    self.assertEqual(
      ['foo', '  foo_0_0', '  foo_0_1'],
      manager.list(firestore=self.mock_firestore, report='bar',
                   file='bar.list', **STANDARD_ARGS))

  def test_add_basic(self):
    manager = ReportManagerTest._Manager()
    manager.type = Type.GA360_RPT
    manager._read_json = mock.Mock(return_value=REPORT_CONFIG)
    self.mock_firestore.update_document.return_value = None
    manager._lazy_firestore = self.mock_firestore

    manager.add(firestore=self.mock_firestore, report='bar',
                file='bar.list', **STANDARD_ARGS)
    self.assertEqual(1, self.mock_firestore.update_document.call_count)

  def test_add_no_content(self):
    manager = ReportManagerTest._Manager()
    manager.type = Type.GA360_RPT
    manager._read_json = mock.Mock(return_value=None)
    self.mock_firestore.update_document.return_value = None

    manager.add(firestore=self.mock_firestore, report='bar',
                file='bar.list', **STANDARD_ARGS)
    self.assertEqual(0, self.mock_firestore.update_document.call_count)

  def test_delete_valid(self):
    manager = ReportManagerTest._Manager()
    manager.type = Type.GA360_RPT
    self.mock_firestore.delete_document.return_value = None
    manager._read_email = mock.Mock(return_value='luke@skywalker.com')
    mock_scheduler = mock.create_autospec(scheduler.Scheduler)
    mock_scheduler.process.side_effect = [
      [{'name': 'location/us-east1/thingy/bar_1'},
       {'name': 'location/us-east1/thingy/bar_2'},],
      None,
      None
    ]
    manager._lazy_scheduler = mock_scheduler
    manager._lazy_firestore = self.mock_firestore

    manager.delete(firestore=self.mock_firestore, report='bar',
                   file='bar.list', **STANDARD_ARGS)

    self.assertEqual(1, self.mock_firestore.delete_document.call_count)
    self.assertEqual([
      mock.call({'action': 'list',
                 'email': 'luke@skywalker.com',
                 'project': None,
                 'html': False}),
      mock.call({'action': 'disable',
                 'email': 'luke@skywalker.com',
                 'project': None,
                 'job_id': 'bar_1'}),
      mock.call({'action': 'disable',
                 'email': 'luke@skywalker.com',
                 'project': None,
                 'job_id': 'bar_2'})],
      mock_scheduler.process.call_args_list)

  def test_delete_no_scheduler(self):
    manager = ReportManagerTest._Manager()
    manager.type = Type.GA360_RPT
    self.mock_firestore.delete_document.return_value = None
    manager._read_email = mock.Mock(return_value='luke@skywalker.com')
    manager._lazy_scheduler = None
    manager._lazy_firestore = self.mock_firestore

    manager.delete(firestore=self.mock_firestore, report='bar',
                   file='bar.list', **STANDARD_ARGS)
    self.assertEqual(1, self.mock_firestore.delete_document.call_count)

  def test_delete_no_email_in_file(self):
    with mock.patch.object(logging, 'error') as mock_logger:
      manager = ReportManagerTest._Manager()
      manager.type = Type.GA360_RPT
      self.mock_firestore.delete_document.return_value = None
      manager._read_email = mock.Mock(return_value=None)
      manager._lazy_firestore = self.mock_firestore

      manager.delete(firestore=self.mock_firestore, report='bar',
                    file='bar.list', **STANDARD_ARGS)
      self.assertEqual(1, self.mock_firestore.delete_document.call_count)
      self.assertEqual(1, mock_logger.call_count)
      self.assertEqual(mock.call('No email found, cannot access scheduler.'),
                       mock_logger.call_args)
