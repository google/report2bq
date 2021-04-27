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
from copy import deepcopy
import unittest

from classes import strip_nulls


VALID_DICT = {
  "a": "1",
  "b": 2,
  "c": {
    "aa": "1",
  },
  "d": [
    "aaa", "bbb", "ccc",
  ]
}


class InitTest(unittest.TestCase):

  def test_strip_nulls_valid(self):
    invalid_dict = deepcopy(VALID_DICT)
    invalid_dict['c'].update({'bb': None})
    invalid_dict.update({'bad': None,})

    self.assertEqual(VALID_DICT, strip_nulls(invalid_dict))

  def test_strip_nulls_empty_dict(self):
    self.assertEqual({}, strip_nulls({}))

  def test_strip_nulls_empty_list(self):
    self.assertEqual([], strip_nulls([]))

  def test_strip_nulls_none(self):
    self.assertEqual(None, strip_nulls(None))
