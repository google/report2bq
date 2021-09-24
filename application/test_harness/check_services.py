# Copyright 2020 Google LLC
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
from __future__ import annotations

from absl import app
from absl import flags
from classes.api_checker import APIService
from google.cloud import service_usage

FLAGS = flags.FLAGS
flags.DEFINE_string('project', None, 'GCP Project.')
flags.DEFINE_multi_string('api', None,
                          'Cloud API whose status is to be inspected.')


def main(unused) -> None:
  del unused
  service = APIService(FLAGS.project)

  for api in FLAGS.api:
    print((f'{api}: '
          f'{"enabled" if service.check_api(api) else "disabled"}'))


if __name__ == '__main__':
  app.run(main)
