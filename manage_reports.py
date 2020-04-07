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

from classes.dcm import DCM


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
flags.DEFINE_integer('account',
                     None,
                     'CM account id. Required for CM Superusers.')

flags.register_multi_flags_validator(['account', 'profile'],
                       lambda value: (value.get('account') == None and not value.get('profile') == None),
                       message='--account requires a --profile to be set.')
flags.register_multi_flags_validator(['report_id', 'list'],
                       lambda value: value.get('report_id') == None and not value.get('list'),
                       message='--list does not take a report_id')
flags.register_multi_flags_validator(['report_id', 'restore'],
                       lambda value: value.get('report_id') == None and value.get('restore'),
                       message='--restore does not take a report_id')

flags.mark_flag_as_required('email')
flags.mark_flag_as_required('project')

flags.mark_bool_flags_as_mutual_exclusive(['list', 'backup', 'restore'])


# Stub main()
def main(unused_argv):
  if FLAGS.profile:
    # DCM
    dcm = DCM(superuser=(FLAGS.profile and FLAGS.account), email=FLAGS.email, project=FLAGS.project)

    if FLAGS.list:
      reports = dcm.get_reports(profile_id=FLAGS.profile, account_id=FLAGS.account)
      if reports:
        print('Report list for profile id {profile}'.format(profile=FLAGS.profile))
        print('')
        for report in reports.get('items'):
          print('ID [{id}], "{name}". Type [{type}], running {run}'.format(
            id=report['id'],
            name=report['name'],
            type=report['type'],
            run=report['schedule']['repeats'] if report['schedule']['active'] else 'MANUAL'
          ))

    if FLAGS.backup:
      report = dcm.get_report_definition(profile_id=FLAGS.profile, report_id=FLAGS.report_id, account_id=FLAGS.account)
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
    print('DV360 not implemented yet')


if __name__ == '__main__':
  app.run(main)

