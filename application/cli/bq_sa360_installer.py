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

import base64
import logging
import os

from absl import app
from absl import flags
from contextlib import suppress
from datetime import datetime

from main import sa360_report_creator

logging.basicConfig(
  filename=f'bq_sa360_installer-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log',
  format='%(asctime)s %(message)s',
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.DEBUG
)

FLAGS = flags.FLAGS
flags.DEFINE_string('project', None,
                    'GCP Project to act on. Default is the environment value.')
flags.DEFINE_string('email', None, 'Your email.')


def main(unused_argv):
  project = FLAGS.project or os.environ('GCP_PROJECT')
  event = {
    'data': base64.b64encode('RUN'.encode('utf-8')),
    'attributes': {
      'project': project,
      'email': FLAGS.email,
    }
  }
  sa360_report_creator(event, None)


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
