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
import os

# Class Imports
from absl import app
from absl import flags
from contextlib import suppress
from datetime import datetime
from urllib.parse import unquote

from classes.report2bq import Report2BQ
from classes.report_type import Type
from main import report_fetch, report_runner

logging.basicConfig(
  filename=f'report2bq-test-harness-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log',
  format='%(asctime)s %(message)s',
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.DEBUG
)

FLAGS = flags.FLAGS
flags.DEFINE_integer('dv360_id',
                     None,
                     'Report to load')
flags.DEFINE_integer('cm_id',
                     None,
                     'Report to load')
flags.DEFINE_string('report_id',
                     None,
                     'Report to load')
flags.DEFINE_string('sa360_url',
                    None,
                    'SA360 report URL')

flags.DEFINE_string('product', None, 'Product type')
flags.DEFINE_boolean('force',
                     False,
                     'Force update, regardless of last update time')
flags.DEFINE_boolean('rebuild_schema',
                     False,
                     'Rescan the file for schema')
flags.DEFINE_boolean('infer_schema',
                     False,
                     'Infer the DB schema [ALPHA]')
flags.DEFINE_boolean('append',
                     False,
                     'Append the data to the existing table instead of replacing.')


flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'GCP Project')

flags.DEFINE_string('dest_project',
                     None,
                     'Destination BQ Project')
flags.DEFINE_string('dest_dataset',
                     None,
                     'Destination BQ Dataset')

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

flags.DEFINE_string('notify_message',
                     None,
                     'Body of post job completion message')
flags.DEFINE_string('notify_topic',
                     None,
                     'Post load job completion topic to notify')

flags.DEFINE_boolean('partition',
                     False,
                     'Create a partitioned table in BQ.')
flags.DEFINE_boolean('development', False, 'Leave the files in GCS.')
flags.DEFINE_boolean('runner', False, 'Runner, not fetcher')


# Stub main()
def main(unused_argv):
  attributes = {
    'list_reports': FLAGS.list,
    'dv360': FLAGS.dv360,
    'cm': FLAGS.cm,
    'force': FLAGS.force,
    'dv360_id': FLAGS.dv360_id,
    'cm_id': FLAGS.cm_id,
    'report_id': FLAGS.report_id,
    'profile': FLAGS.profile,
    'account_id': FLAGS.account,
    'email': FLAGS.email,
    'in_cloud': True,
    'append': FLAGS.append,
    'project': FLAGS.project or os.environ.get('GCP_PROJECT'),
    'sa360_url': unquote(FLAGS.sa360_url) if FLAGS.sa360_url else None,
    'sa360': (True if FLAGS.sa360_url else False),
    'dest_project': FLAGS.dest_project,
    'dest_dataset': FLAGS.dest_dataset,
    'infer_schema': FLAGS.infer_schema,
    # 'product': Type(FLAGS.product),
    # 'type': FLAGS.product,
    'notify_topic': FLAGS.notify_topic,
    'notify_message': FLAGS.notify_message,
    'partition': FLAGS.partition,
    'development': FLAGS.development,
  }
  if FLAGS.product:
    attributes['type'] = Type(FLAGS.product)
  elif attributes.get('sa360_url'):
    attributes['type'] = Type.SA360
  elif attributes.get('profile'):
    attributes['type'] = Type.CM
  else:
    attributes['type'] = Type.DV360

  if Type(FLAGS.product) == Type.SA360_RPT:
    f = report_runner
  else:
    f = report_fetch if not FLAGS.runner else report_runner

  f({'attributes': attributes}, None)


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
