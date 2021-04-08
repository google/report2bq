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

__author__ = ['davidharcombe@google.com (David Harcombe)']

from classes import discovery
from classes.services import Service
from classes.sa360_report_validation.sa360_validator_factory import SA360ValidatorFactory
from classes.sa360_report_manager import SA360Manager
from classes.sa360_reports import SA360ReportTemplate
import json
import pprint
import uuid

from absl import app, flags

from classes.firestore import Firestore, firestore
from classes.report_type import Type
from classes.sa360_report_runner import SA360ReportRunner
from classes.sa360_dynamic import SA360Dynamic
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
  #   ready = SA360ReportTemplate().prepare(
  #     template=template,
  #     values={
  #       "report": "holiday_2020",
  #       "dest_dataset": "holiday_2020_us",
  #       "timezone": "America/Toronto",
  #       "email": "davidharcombe@google.com",
  #       "AgencyId": "20700000001285329",
  #       "AdvertiserId": "21700000001634678",
  #       "ConversionMetric": "HBD - 360i - Confirmation Page (Transactions)",
  #       "RevenueMetric": "HBD - 360i - Confirmation Page (Revenue)",
  #       "offset": 0,
  #       "lookback": 0,
  #       "notifier": {
  #         "topic": "postprocessor",
  #         "message": "holiday_2020"
  #       },
  #       "agencyName": "Vineyard Vines",
  #       "advertiserName": "Vineyard Vines",
  #       "minute": "2"
  #     })

  # Add new reports
  scheduler = Scheduler()
  with open(FLAGS.file) as reports:
    runners = json.loads(''.join(reports.readlines()))
    firestore = Firestore()

    sa360_report_definitions = firestore.get_document(Type.SA360_RPT, '_reports')
    sa360_manager = SA360Manager()
    sa360_manager.sa360 = SA360Dynamic(email='davidharcombe@google.com', project="report2bq-zz9-plural-z-alpha")
    sa360_manager.sa360_service = sa360_manager.sa360.service()

    for runner in runners:
      id = f"{runner['report']}_{runner['AgencyId']}_{runner['AdvertiserId']}"
      sa360_manager.validator_factory = SA360ValidatorFactory()
      (valid, validity) = sa360_manager._file_based(
        project='report2bq-zz9-plural-z-alpha',
        sa360_report_definitions=sa360_report_definitions, report=runner)
      # runner.update(validity)
      firestore.store_document(Type.SA360_RPT, f'{id}', runner)

      if valid:
        print(f'Valid report: {id}')

        args = {
            'action': 'delete',
            'email': runner['email'],
            'project': 'report2bq-zz9-plural-z-alpha',
            'sa360_id': id,
        }
        try:
          scheduler.process(args)

        except Exception as e:
          print(e)

        args = {
          'action': 'create',
          'email': runner['email'],
          'project': 'report2bq-zz9-plural-z-alpha',
          'force': False,
          'infer_schema': False,
          'append': False,
          'sa360_id': id,
          'description': f'[US] SA360 Hourly Depleted: {runner["agencyName"]}/{runner["advertiserName"]}',
          'dest_dataset': 'sa360_hourly_depleted_us',
          'minute': runner['minute'],
        }
        try:
          scheduler.process(args)

        except Exception as e:
          print(e)

      else:
        print(f'Invalid report: {id}')
        firestore.store_document(Type.SA360_RPT, f'{id}', runner)

  # runner = Firestore().get_document(Type.SA360_RPT, 'holiday_2020_20700000000000942_21700000001478610')
  # runners = Firestore().get_all_reports(Type.SA360_RPT)

  # Run SA360 Report
  # runner = SA360ReportRunner(report_id='holiday_2020_20100000000000931_21700000001010531', email='davidharcombe@google.com', project='report2bq-zz9-plural-z-alpha')
  # runner = SA360ReportRunner(report_id='holiday_2020_20700000001201701_21700000001667419', email='davidharcombe@google.com', project='report2bq-zz9-plural-z-alpha')
  # runner = SA360ReportRunner(
  #   report_id='holiday_2020_20700000001001042_21700000001677026',
  #   email='davidharcombe@google.com',
  #   project='report2bq-zz9-plural-z-alpha')
  # if r := runner.run():
  #   sa360 = SA360Dynamic(email='davidharcombe@google.com',
  #                        project='report2bq-zz9-plural-z-alpha')
  #   sa360.handle_report(r)


if __name__ == '__main__':
  app.run(main)
