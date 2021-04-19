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

from main import post_processor

logging.basicConfig(
  filename=f'postprocessor-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log',
  format='%(asctime)s %(message)s',
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.DEBUG
)

FLAGS = flags.FLAGS
flags.DEFINE_string('name',
                    None,
                    'filename')
flags.DEFINE_string('project', None,
                    'GCP Project to act on. Default is the environment value.')
flags.DEFINE_string('dataset', 'report2bq',
                    'BQ Dataset to act on. Default is "report2bq".')
flags.DEFINE_string('table', None,
                    'BQ Table to act on.')
flags.DEFINE_string('report_id', None, 'The report id that caused this.')
flags.DEFINE_string('product', None, 'The report type.')
flags.DEFINE_integer('rows', 0, 'Number of rows imported the table.')
flags.DEFINE_string('columns', None,
                    'A ";" delimited list of columns in the table.')


def main(unused_argv):
  project = FLAGS.project or os.environ('GCP_PROJECT')
  event = {
    'data': base64.b64encode(FLAGS.name.encode('utf-8')),
    'attributes': {
      'project': project,
      'dataset': FLAGS.dataset,
      'table': FLAGS.table,
      'rows': str(FLAGS.rows),
      'id': FLAGS.report_id,
      'type': FLAGS.product,
      'columns': FLAGS.columns
    }
  }
  post_processor(event, None)


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
