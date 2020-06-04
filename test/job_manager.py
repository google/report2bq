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

# Python logging
import logging
import pprint
import random
import uuid

# Class Imports
from absl import app
from absl import flags
from io import StringIO

from classes.credentials import Credentials
from classes.scheduler import Scheduler


FLAGS = flags.FLAGS

flags.DEFINE_string('email',
                    None,
                    'Report owner/user email')

flags.DEFINE_string('project',
                    None,
                    'Project id')

flags.DEFINE_string('job_id',
                    None,
                    'Job id to use')

flags.DEFINE_string('dest_project', None, '')
flags.DEFINE_string('dest_dataset', None, '')
flags.DEFINE_string('description', '', 'Description')

flags.DEFINE_string('report_id','', 'Report')
flags.DEFINE_string('profile', None, '')

flags.DEFINE_string('sa360_url', None, '')

flags.DEFINE_string('adh_customer', None, '')
flags.DEFINE_string('adh_query', None, '')
flags.DEFINE_string('days', '60', '')
flags.DEFINE_string('api_key', None, '')

flags.DEFINE_string('minute', None, '')
flags.DEFINE_string('hour', None, '')
flags.DEFINE_string('timezone', None, '')

flags.DEFINE_boolean('force', False, '')
flags.DEFINE_boolean('append', False, '')
flags.DEFINE_boolean('runner', False, '')
flags.DEFINE_boolean('infer_schema', False, '')

flags.DEFINE_boolean('list', False, 'List the jobs')
flags.DEFINE_boolean('delete', False, 'Delete the job')
flags.DEFINE_boolean('create', False, 'Create a job')


def main(unused_argv):
  args = FLAGS.flag_values_dict()
  if FLAGS.list: args['action'] = 'list'
  if FLAGS.delete: args['action'] = 'delete'
  if FLAGS.create: args['action'] = 'create'

  scheduler = Scheduler()
  print(scheduler.process(args).replace('<br/>', '\n'))


if __name__ == '__main__':
  app.run(main)