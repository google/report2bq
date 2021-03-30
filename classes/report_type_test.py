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

from classes.report_type import Type


class TypeTest(unittest.TestCase):

  def setUp(self):
    pass

  def test_valid_current_enum(self):
    self.assertEqual(Type.DV360, Type('dv360'))

  def test_valid_old_enum(self):
    self.assertEqual([Type.DV360, Type.CM], [ Type(T) for T in ['dbm', 'dcm'] ])

  def test_valid_internals(self):
    self.assertEqual([Type._ADMIN, Type._JOBS, Type._RUNNING],
                     [ Type(T) for T in ['administration', 'jobs', 'running'] ])

  def test_unknown(self):
    self.assertEqual([Type._UNKNOWN, Type._UNKNOWN],
                     [ Type(T) for T in ['foo', None] ])

  def test_runner(self):
    self.assertEqual('run-dv360-123', Type.DV360.runner('123'))
    self.assertEqual(None, Type._ADMIN.runner('123'))

  def test_str(self):
    self.assertEqual(['dv360', 'cm', 'administration', 'unknown'],
                     [ T.value for T in [
                         Type.DV360, Type.CM, Type._ADMIN, Type(None),
                       ] ])
