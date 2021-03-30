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

# Class Imports
from absl import app
from absl import flags
from pprint import pprint
from typing import Dict, Any

from classes import ReportFetcher
from classes import fetcher_factory
from classes.report_type import Type
# from classes.dcm import DCM


FLAGS = flags.FLAGS
flags.DEFINE_integer('report_id',
                     None,
                     'Report to process')

flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'GCP Project')
flags.DEFINE_string('product',
                     'dv360',
                     'DV360 or CM')

flags.DEFINE_boolean('list',
                     False,
                     'List available reports')

flags.DEFINE_boolean('backup',
                     False,
                     'Backup listed report')

flags.DEFINE_boolean('restore',
                     False,
                     'Restore listed report')
flags.DEFINE_string('new_name',
                    None,
                    'New report name')

flags.DEFINE_integer('profile',
                     None,
                     'Campaign Manager profile id. Only needed for CM.')

flags.mark_flag_as_required('email')
flags.mark_flag_as_required('project')

flags.mark_bool_flags_as_mutual_exclusive(['list', 'backup', 'restore'])


# Stub main()
def main(unused_argv):
  fetcher = fetcher_factory.create_fetcher(Type(FLAGS.product),
                                           email=FLAGS.email,
                                           project=FLAGS.project,
                                           profile=FLAGS.profile)

  if fetcher.report_type == Type.CM:
    if FLAGS.list:
      reports = fetcher.get_reports()
      if reports:
        print('Report list')
        print('')
        for report in reports:
          print('ID [{id}] on profile [{profile}], "{name}". Type [{type}], running {run}'.format(
            id=report['id'],
            name=report['name'],
            type=report['type'],
            run=report['schedule']['repeats'] if report['schedule']['active'] else 'MANUAL',
            profile=report['ownerProfileId']
          ))

    if FLAGS.backup:
      report = fetcher.get_report_definition(profile_id=FLAGS.profile, report_id=FLAGS.report_id,)
      with open('config_files/{report}.json'.format(report=FLAGS.report_id), 'w') as report_file:
        report_file.write(json.dumps(report, indent=2))
        report_file.close()

    if FLAGS.restore:
      keys_wanted = [
        'format',
        'name',
        'criteria',
        'schedule',
        'delivery'
      ]

      new_report = {}
      with open('config_files/{report}.json'.format(report=FLAGS.report_id)) as report_file:
        report = json.load(report_file)
        for key in report:
          if key in keys_wanted:
            new_report[key] = report[key]

        if FLAGS.new_name:
          new_report['name'] = FLAGS.new_name

        pprint(new_report)

  else:
    # DV360
    if FLAGS.list:
      reports = fetcher.get_reports()
      if reports:
        print('Report list')
        print('')
        for report in reports.get('queries'):
          print('ID [{id}], "{name}". Type [{type}], running {run}'.format(
            id=report['queryId'],
            name=report['metadata']['title'],
            type=report['params']['type'],
            run=report['schedule']['frequency'] if 'schedule' in report else 'MANUAL'
          ))

    # print('DV360 not implemented yet')


if __name__ == '__main__':
  app.run(main)

