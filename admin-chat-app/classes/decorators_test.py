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

from . import decorators


class LazyPropertyTest(unittest.TestCase):
  class Foo(object):
    @decorators.lazy_property
    def lazy_thing(self) -> str:
      return 'lazy'

  def test_lazy_thing(self):
    foo = LazyPropertyTest.Foo()
    self.assertFalse(hasattr(foo, '_lazy_lazy_thing'))
    self.assertEqual('lazy', foo.lazy_thing)
    self.assertTrue(hasattr(foo, '_lazy_lazy_thing'))


if __name__ == '__main__':
  unittest.main()
