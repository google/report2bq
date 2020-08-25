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
import pprint
import uuid

from absl import app, flags

from classes.firestore import Firestore
from classes.report_type import Type
from classes.sa360_report_runner import SA360ReportRunner
from classes.sa360_v2 import SA360
from classes.scheduler import Scheduler

"""
Currently Non functional, this can be used to manually install the json for SA360 Dynamic Reports.

Better system coming.
"""
FLAGS = flags.FLAGS
flags.DEFINE_string('file', None, 'JSON file containing the report definition.')


def main(unusedargv):
  scheduler = Scheduler()
  # with open(FLAGS.file) as reports:
  #   runners = json.loads(''.join(reports.readlines()))

  #   for runner in runners:
  #     id = f"{runner['report']}_{runner['AgencyId']}_{runner['AdvertiserId']}"
  #     Firestore().update_document(Type.SA360_RPT, f'{id}', runner)

  #     args = {
  #       'action': 'create',
  #       'email': runner['email'],
  #       'project': 'report2bq-zz9-plural-z-alpha',
  #       'force': False,
  #       'infer_schema': True,
  #       'append': False,
  #       'sa360_id': id,
  #       'description': 'SA360 disableable job',
  #     }
  #     scheduler.process(args)


  runner = Firestore().get_document(Type.SA360_RPT, 'holiday_2020_20700000001201701_21700000001494948')
  runners = Firestore().get_all_reports(Type.SA360_RPT)
  runner = SA360ReportRunner(report_id='holiday_2020_20700000001201701_21700000001494948', email='davidharcombe@google.com', project='report2bq-zz9-plural-z-alpha')
  r = runner.run()

  # sa360 = SA360(email='davidharcombe@google.com', project='galvanic-card-234919')
  # sa360.handle_offline_report(r)

if __name__ == '__main__':
  app.run(main)