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


if 'unittest.util' in __import__('sys').modules:
    # Show full diff in self.assertEqual.
    __import__('sys').modules['unittest.util']._MAX_LENGTH = 999999999


class MockValidator(object):
    def __init__(self, validator):
        self.validator = validator

    def __eq__(self, other):
        return bool(self.validator(other))

STANDARD_ARGS = {
  'gcs_stored': True,
  'email': 'luke@skywalker.com',
  'project': 'test',
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

RUNNER =  {
  "description": "Test job #1",
  "report": "bmo_test",
  "email": "davidharcombe@google.com",
  "view_id": "00001",
  "minute": 10,
  "date_ranges": [{"start_date": "7daysAgo", "end_date": "yesterday"}]
}

class ReportManagerTest(unittest.TestCase):

  class _Manager(ReportManager):
    report_type = Type.GA360_RPT
    bucket = 'manager-bucket'
    project = 'test'


  def setUp(self):
    self.mock_firestore = mock.create_autospec(firestore.Firestore)

  def test_list_valid(self):
    manager = ReportManagerTest._Manager()
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
    manager._read_json = mock.Mock(return_value=REPORT_CONFIG)
    self.mock_firestore.update_document.return_value = None
    manager._lazy_firestore = self.mock_firestore

    manager.add(firestore=self.mock_firestore, report='bar',
                file='bar.list', **STANDARD_ARGS)
    self.assertEqual(1, self.mock_firestore.update_document.call_count)

  def test_add_no_content(self):
    manager = ReportManagerTest._Manager()
    manager._read_json = mock.Mock(return_value=None)
    self.mock_firestore.update_document.return_value = None

    manager.add(firestore=self.mock_firestore, report='bar',
                file='bar.list', **STANDARD_ARGS)
    self.assertEqual(0, self.mock_firestore.update_document.call_count)

  def test_delete_valid(self):
    self.maxDiff = None
    manager = ReportManagerTest._Manager()
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
      mock.call(**{'action': 'list',
                 'email': 'luke@skywalker.com',
                 'project': 'test',
                 'html': False}),
      mock.call(**{'action': 'disable',
                 'email': 'luke@skywalker.com',
                 'project': 'test',
                 'job_id': 'bar_1'}),
      mock.call(**{'action': 'disable',
                 'email': 'luke@skywalker.com',
                 'project': 'test',
                 'job_id': 'bar_2'})],
      mock_scheduler.process.call_args_list)

  def test_delete_no_scheduler(self):
    manager = ReportManagerTest._Manager()
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
      self.mock_firestore.delete_document.return_value = None
      manager._read_email = mock.Mock(return_value=None)
      manager._lazy_firestore = self.mock_firestore

      manager.delete(firestore=self.mock_firestore, report='bar',
                    file='bar.list', **STANDARD_ARGS)
      self.assertEqual(1, self.mock_firestore.delete_document.call_count)
      self.assertEqual(1, mock_logger.call_count)
      self.assertEqual(mock.call('No email found, cannot access scheduler.'),
                       mock_logger.call_args)

  def test_valid_schedule_new_job(self):
    manager = ReportManagerTest._Manager()
    manager.report_type = Type.GA360_RPT
    self.mock_firestore.update_document.return_value = None
    mock_scheduler = mock.create_autospec(scheduler.Scheduler)
    mock_scheduler.process.side_effect = [
      (False, None),
      None
    ]
    manager._lazy_scheduler = mock_scheduler
    manager._lazy_firestore = self.mock_firestore

    result = manager._schedule_job(project='rebellion',
                                   runner=RUNNER,
                                   id='r2d2')
    self.assertEqual('run-ga360_report-r2d2 - Valid and installed.',
                     result)
    self.assertEqual([
      mock.call(**{'action': 'get', 'email': 'davidharcombe@google.com',
                 'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
      mock.call(**{'action': 'create', 'email': 'davidharcombe@google.com',
                 'project': 'rebellion', 'force': False, 'infer_schema': False,
                 'append': False, 'report_id': 'r2d2',
                 'description': 'Test job #1', 'minute': 10, 'hour': '*',
                 'type': Type.GA360_RPT})],
      mock_scheduler.process.call_args_list)

  def test_valid_reschedule_existing_job(self):
    manager = ReportManagerTest._Manager()
    manager.report_type = Type.GA360_RPT
    self.mock_firestore.update_document.return_value = None
    mock_scheduler = mock.create_autospec(scheduler.Scheduler)
    mock_scheduler.process.side_effect = [
      (True, None),
      None,
      None
    ]
    manager._lazy_scheduler = mock_scheduler
    manager._lazy_firestore = self.mock_firestore

    result = manager._schedule_job(project='rebellion',
                                   runner=RUNNER,
                                   id='r2d2')
    self.assertEqual('run-ga360_report-r2d2 - Valid and installed.',
                     result)
    self.assertEqual([
      mock.call(**{'action': 'get', 'email': 'davidharcombe@google.com',
                 'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
      mock.call(**{'action': 'delete', 'email': 'davidharcombe@google.com',
                 'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
      mock.call(**{'action': 'create', 'email': 'davidharcombe@google.com',
                 'project': 'rebellion', 'force': False, 'infer_schema': False,
                 'append': False, 'report_id': 'r2d2',
                 'description': 'Test job #1', 'minute': 10, 'hour': '*',
                 'type': Type.GA360_RPT})],
      mock_scheduler.process.call_args_list)

  def test_error_cant_delete_existing_job(self):
    with mock.patch.object(logging, 'error') as mock_logger:
      manager = ReportManagerTest._Manager()
      manager.report_type = Type.GA360_RPT
      self.mock_firestore.update_document.return_value = None
      mock_scheduler = mock.create_autospec(scheduler.Scheduler)
      mock_scheduler.process.side_effect = [
        (True, None),
        Exception('403 Invalid OAuth')
      ]
      manager._lazy_scheduler = mock_scheduler
      manager._lazy_firestore = self.mock_firestore

      result = manager._schedule_job(project='rebellion',
                                    runner=RUNNER,
                                    id='r2d2')
      self.assertEqual('run-ga360_report-r2d2 - Already present but delete '
                      'failed: 403 Invalid OAuth',
                      result)
      self.assertEqual([
        mock.call(**{'action': 'get', 'email': 'davidharcombe@google.com',
                   'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
        mock.call(**{'action': 'delete', 'email': 'davidharcombe@google.com',
                   'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
        ],
        mock_scheduler.process.call_args_list)
      self.assertEqual(
        mock.call('%s - Already present but delete failed: %s',
                  'run-ga360_report-r2d2',
                  MockValidator(lambda x: isinstance(x, Exception))),
        mock_logger.call_args)

  def test_error_cant_create_new_job(self):
    with mock.patch.object(logging, 'error') as mock_logger:
      manager = ReportManagerTest._Manager()
      manager.report_type = Type.GA360_RPT
      self.mock_firestore.update_document.return_value = None
      mock_scheduler = mock.create_autospec(scheduler.Scheduler)
      mock_scheduler.process.side_effect = [
        (True, None),
        None,
        Exception('403 Invalid OAuth')
      ]
      manager._lazy_scheduler = mock_scheduler
      manager._lazy_firestore = self.mock_firestore

      result = manager._schedule_job(project='rebellion',
                                    runner=RUNNER,
                                    id='r2d2')
      self.assertEqual('run-ga360_report-r2d2 - Failed to create: '
                       '403 Invalid OAuth',
                       result)
      self.assertEqual([
        mock.call(**{'action': 'get', 'email': 'davidharcombe@google.com',
                   'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
        mock.call(**{'action': 'delete', 'email': 'davidharcombe@google.com',
                   'project': 'rebellion', 'job_id': 'run-ga360_report-r2d2'}),
        mock.call(**{'action': 'create', 'email': 'davidharcombe@google.com',
                   'project': 'rebellion', 'force': False,
                   'infer_schema': False, 'append': False, 'report_id': 'r2d2',
                   'description': 'Test job #1', 'minute': 10, 'hour': '*',
                   'type': Type.GA360_RPT})],
        mock_scheduler.process.call_args_list)
      self.assertEqual(
        mock.call('%s - Failed to create: %s',
                  'run-ga360_report-r2d2',
                  MockValidator(lambda x: isinstance(x, Exception))),
        mock_logger.call_args)
