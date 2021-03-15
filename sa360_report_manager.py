"""
Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

__author__ = [
  'davidharcombe@google.com (David Harcombe)'
]

import json
import logging
import pprint
import main as m

# Class Imports
from absl import app
from absl import flags
from contextlib import suppress
from datetime import datetime
from urllib.parse import unquote

from classes.firestore import Firestore
from classes.report_type import Type
from classes.sa360_report_manager import SA360Manager
from classes.scheduler import Scheduler


logging.basicConfig(
  filename=f'sa360_report_manager-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log',
  format='%(asctime)s %(message)s',
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.DEBUG
)

FLAGS = flags.FLAGS

# actions
flags.DEFINE_bool('list', False, 'List all defined reports.')
flags.DEFINE_bool('show', False, 'Print the defintion of the named report.')
flags.DEFINE_bool(
  'add', False, 'Add a new report from an SA360 definition format JSON file.')
flags.DEFINE_bool(
  'delete', False,
  'Remove a defined report. This will also disable any runners defined for this report.')
flags.DEFINE_bool(
  'install', False, 'Add runners for a named report from a JSON file.')
flags.DEFINE_bool('validate', False, 'Validate a defined report.')

# add
flags.DEFINE_string('file', None, 'JSON file containing the report definition.')
flags.DEFINE_bool('gcs_stored', False, 'Is this stored in gcs?')

# add/delete/show
flags.DEFINE_string(
  'name', None,
  'Name as which the report should be stored. Default is the file name minus extension.')

# common
flags.DEFINE_string(
  'project', None, 'GCP Project act on. Default is the environment default.')
flags.DEFINE_string('email', None, 'Report owner/user email.')
flags.DEFINE_string('api_key', None, 'API Key for scheduler.')


def main(unused_argv):
  if FLAGS.list: action = 'list'
  elif FLAGS.show: action = 'show'
  elif FLAGS.add: action = 'add'
  elif FLAGS.install: action = 'install'
  elif FLAGS.delete: action = 'delete'
  elif FLAGS.validate: action = 'validate'
  else: raise NotImplementedError()

  # event = {
  #   'bucket': 'report2bq-zz9-plural-z-alpha',
  #   'name': FLAGS.file,
  #   'action': action,
  #   **{k: v for k, v in FLAGS.flag_values_dict().items() if v is not None}
  # }
  # m.sa360_report_manager(event)
  args = {
    'action': action,
    '_print': True,
    **{k: v for k, v in FLAGS.flag_values_dict().items() if v is not None}
  }
  SA360Manager().manage(**args)


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
