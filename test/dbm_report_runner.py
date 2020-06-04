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

# Class Imports
from absl import app
from absl import flags

from classes.dbm_report_runner import DBMReportRunner
from classes.report2bq import Report2BQ
from datetime import datetime

FLAGS = flags.FLAGS
flags.DEFINE_integer('dbm_id',
                           None,
                           'Report to load')

flags.DEFINE_string('project',
                     None,
                     'Project')

flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')


logging.basicConfig(
  filename=('dv360-blast-%s.log' % (datetime.now().strftime("%Y-%m-%d-%H:%M:%S"))), 
    format='%(asctime)s %(message)s', 
    datefmt='%Y-%m-%d %I:%M:%S %p',
    level=logging.INFO
)

# Stub main()
def main(unused_argv):
  runner = DBMReportRunner(
    dbm_id=FLAGS.dbm_id, 
    email=FLAGS.email,
    project=FLAGS.project
  )
  runner.run()


if __name__ == '__main__':
  app.run(main)
