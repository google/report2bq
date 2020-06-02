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

from classes.firestore import Firestore
from classes.report_type import Type
from classes.sa360_report_runner import SA360ReportRunner
from classes.sa360_v2 import SA360

"""
Currently Non functional, this can be used to manually install the json for SA360 Dynamic Reports.

Better system coming.
"""
# with open('config_files/sa360_reports.json') as reports:
#   cfg = json.loads(''.join(reports.readlines()))
#   pprint.pprint(cfg, compact=False, indent=2)

#   Firestore().update_document(Type.SA360_RPT, '_reports', cfg)

# with open('config_files/sa360_runners.json') as reports:
#   cfg = json.loads(''.join(reports.readlines()))
#   pprint.pprint(cfg, compact=False, indent=2)

#   for _cfg in cfg:
#     Firestore().update_document(Type.SA360_RPT, f"{uuid.uuid4()}", _cfg)

# runner = Firestore().get_document(Type.SA360_RPT, '7f403d7e-2ba6-47e9-bcc3-f5d3cecd9c8e')
# runners = Firestore().get_all_reports(Type.SA360_RPT)
# runner = SA360ReportRunner('7f403d7e-2ba6-47e9-bcc3-f5d3cecd9c8e', None, 'davidharcombe@google.com', 'galvanic-card-234919')
# r = runner.run()

# sa360 = SA360(email='davidharcombe@google.com', project='galvanic-card-234919')
# sa360.handle_offline_report(r)