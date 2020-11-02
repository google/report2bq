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

from classes.sa360_report_manager import SA360Manager
from classes.sa360_reports import SA360ReportTemplate
import json
import pprint
import uuid

from absl import app, flags

from classes.firestore import Firestore, firestore
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
  # Test report prepare
  # with open(FLAGS.file) as template_file:
  #   template = json.loads(''.join(template_file.readlines()))
  #   ready = SA360ReportTemplate().prepare(template=template, values={
  #     "report": "holiday_2020",
  #     "timezone": "America/Toronto",
  #     "email": "davidharcombe@google.com",
  #     "AgencyId": "20700000001001042",
  #     "AdvertiserId": "21700000001533241",
  #     "ConversionMetric": { "value": "AW Conversions", "type": "columnName" },
  #     "RevenueMetric": "ROAS GA",
  #     "offset": 0,
  #     "lookback": 0,
  #     "notifier": {
  #       "topic": "post-processor",
  #       "message": "report2bq_execute_metrics_calculation"
  #     },
  #     "agencyName": "Best Buy Canada",
  #     "advertiserName": "Best Buy Canada Search Corporate",
  #     "minute": "1"
  #   })

  # Add new reports
  scheduler = Scheduler()
  with open(FLAGS.file) as reports:
    runners = json.loads(''.join(reports.readlines()))
    firestore = Firestore()

    sa360_report_definitions = firestore.get_document(Type.SA360_RPT, '_reports')
    sa360_manager = SA360Manager()
    sa360_manager.sa360 = SA360(email='davidharcombe@google.com', project="report2bq-zz9-plural-z-alpha")

    for runner in runners:
      id = f"{runner['report']}_{runner['AgencyId']}_{runner['AdvertiserId']}"
      firestore.store_document(Type.SA360_RPT, f'{id}', runner)

      if sa360_manager._file_based(
        project='report2bq-zz9-plural-z-alpha',
         sa360_report_definitions=sa360_report_definitions, report=runner):
        print(f'Valid report: {id}')
        args = {
          'action': 'create',
          'email': runner['email'],
          'project': 'report2bq-zz9-plural-z-alpha',
          'force': False,
          'infer_schema': False,
          'append': False,
          'sa360_id': id,
          'description': f'[CA] Holiday 2020: {runner["agencyName"]}/{runner["advertiserName"]}',
          'dest_dataset': 'holiday_2020_ca',
          'minute': runner['minute'],
        }
        scheduler.process(args)
      
      else:
        print(f'Invalid report: {id}')

  # runner = Firestore().get_document(Type.SA360_RPT, 'holiday_2020_20700000000000942_21700000001478610')
  # runners = Firestore().get_all_reports(Type.SA360_RPT)

  # Run SA360 Report
  # runner = SA360ReportRunner(report_id='holiday_2020_20100000000000931_21700000001010531', email='davidharcombe@google.com', project='report2bq-zz9-plural-z-alpha')
  # runner = SA360ReportRunner(report_id='holiday_2020_20700000001201701_21700000001667419', email='davidharcombe@google.com', project='report2bq-zz9-plural-z-alpha')
  # if r := runner.run():
  #   sa360 = SA360(email='davidharcombe@google.com', project='report2bq-zz9-plural-z-alpha')
  #   sa360.handle_offline_report(r)

if __name__ == '__main__':
  app.run(main)