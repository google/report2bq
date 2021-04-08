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
from classes.ga360_report_runner import GA360ReportRunner
import logging
import pprint

# Class Imports
from absl import app
from absl import flags

from classes.dcm_report_runner import DCMReportRunner
from classes.dbm_report_runner import DBMReportRunner
from classes.report2bq import Report2BQ
from datetime import datetime

FLAGS = flags.FLAGS
# Mandatory
flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'Project id')

# At least one of
flags.DEFINE_integer('cm_id',
                           None,
                           'Report to load')
flags.DEFINE_integer('dv360_id',
                           None,
                           'Report to load')
flags.DEFINE_string('ga360_id',
                    None,
                    'Report to load')
flags.DEFINE_string('report_id',
                    None,
                    'Report to load')

# Optional
flags.DEFINE_boolean('force',
                     False,
                     'Force update, regardless of last update time')

# Must be present if CM
flags.DEFINE_boolean('cm_profiles',
                     False,
                     'List available CM profiles.')
flags.DEFINE_integer('profile',
                     None,
                     'Campaign Manager profile id. Only needed for CM.')
flags.DEFINE_boolean('cm_superuser',
                     False,
                     'User is an _internal_ CM Superuser.')
flags.DEFINE_integer('account',
                     None,
                     'CM account id. RFequired for CM Superusers.')


# Stub main()
def main(unused_argv):
  if FLAGS.dv360_id:
    runner = DBMReportRunner(
      dbm_id=FLAGS.dv360_id,
      email=FLAGS.email,
      project=FLAGS.project
    )

  if FLAGS.cm_id:
    runner = DCMReportRunner(
      cm_id=FLAGS.cm_id,
      profile=FLAGS.profile,
      email=FLAGS.email,
      project=FLAGS.project
    )

  if FLAGS.ga360_id:
    runner = GA360ReportRunner(
      report_id=FLAGS.ga360_id,
      email=FLAGS.email,
      project=FLAGS.project,
    )

  runner.run()


if __name__ == '__main__':
  app.run(main)
