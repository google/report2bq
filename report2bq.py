"""
Copyright 2018 Google LLC

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
from datetime import datetime
from urllib.parse import unquote

from classes.report2bq import Report2BQ


logging.basicConfig(
  filename=f'sa360uploader-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log', 
  format='%(asctime)s %(message)s', 
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.INFO
)

FLAGS = flags.FLAGS
flags.DEFINE_multi_integer('dv360_id',
                           None,
                           'Report to load')
flags.DEFINE_multi_integer('cm_id',
                           None,
                           'Report to load')
flags.DEFINE_string('sa360_url',
                    None,
                    'SA360 report URL')

flags.DEFINE_boolean('force',
                     False,
                     'Force update, regardless of last update time')
flags.DEFINE_boolean('rebuild_schema',
                     False,
                     'Rescan the file for schema')
flags.DEFINE_boolean('append',
                     False,
                     'Append the data to the existing table instead of replacing.')


flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'GCP Project')

flags.DEFINE_boolean('list',
                     False,
                     'List available reports')
flags.DEFINE_boolean('dv360',
                     False,
                     'List configured DV360 reports')
flags.DEFINE_boolean('cm',
                     False,
                     'List configured CM reports')
flags.DEFINE_boolean('sa360',
                     False,
                     'List configured SA360 reports')

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

flags.register_multi_flags_validator(['account', 'cm_superuser'],
                       lambda value: (value.get('account') and value['cm_superuser']) or 
                                     (not value['cm_superuser'] and not value['account']),
                       message='--account_id must be set for a superuser, and not set for normal users.')
# flags.register_multi_flags_validator(['cm_id', 'profile'],
#                        lambda value: (value['cm_id'] and not value['profile']),
#                        message='profile must be set for a CM report to be specified')                        


# Stub main()
def main(unused_argv):
  reporter = Report2BQ(
    list_reports=FLAGS.list,
    dv360=FLAGS.dv360, 
    cm=FLAGS.cm,
    force=FLAGS.force, 
    rebuild_schema=FLAGS.rebuild_schema,
    dv360_ids=FLAGS.dv360_id,
    cm_ids=FLAGS.cm_id, 
    profile=FLAGS.profile, 
    account_id=FLAGS.account, 
    superuser=FLAGS.cm_superuser, 
    email=FLAGS.email,
    in_cloud=True, 
    append=FLAGS.append,
    project=FLAGS.project,
    sa360_url=unquote(FLAGS.sa360_url),
    sa360=(True if FLAGS.sa360_url else False)
  )
  reporter.run()


if __name__ == '__main__':
  try:
    app.run(main)
  except Exception as e:
    logging.error(e)
