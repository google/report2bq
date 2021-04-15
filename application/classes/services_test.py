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

from classes import services

SA360_DEFINITION = \
  services.ServiceDefinition(name='doubleclicksearch', version='v2')
GMAIL_ARGS = {
  'serviceName': 'gmail',
  'version': 'v1',
}

class ServicesTest(unittest.TestCase):
  def test_valid_service(self):
    self.assertGreater(services.Service.SA360.value, 0)

  def test_single_definition(self):
    self.assertEqual(SA360_DEFINITION, services.Service.SA360.definition)

  def test_single_to_args(self):
    self.assertEqual(GMAIL_ARGS, services.Service.GMAIL.definition.to_args)

  def test_all_definitions(self):
    self.assertEqual(8, len(services.Service))
    for id in range(1, 8):
      S = services.Service(id)
      self.assertEqual(services.SERVICE_DEFINITIONS[S], S.definition)


if __name__ == '__main__':
  unittest.main()
