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

# Python logging
import base64
from datetime import datetime
from classes.dcm import DCM
from classes.report_type import Type
import logging
from holiday2020_sa360_reports import holiday_2020
from main import post_processor

logging.basicConfig(
    filename=f'postprocessor-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log',
    format='%(asctime)s %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S %p',
    level=logging.DEBUG)

event = {
    'data': base64.b64encode('holiday_2020'.encode('utf-8')),
    "attributes": {
        "columns":
            "date;agency;agencyId;advertiser;advertiserId;accountType;account;accountEngineId;campaignEngineId;campaign;campaignType;effectiveBidStrategy;dailyBudget;impr;clicks;cost;GA_Revenue",
        "dataset":
            "holiday_2020_ca",
        "id":
            "holiday_2020_20700000001025614_21700000001456043",
        "project":
            "report2bq-zz9-plural-z-alpha",
        "rows":
            "3506",
        "table":
            "holiday_2020_20700000001025614_21700000001456043_csv",
        "type":
            "sa360_report"
    }
}

h = holiday_2020.Processor()
h.run(**event['attributes'])
# post_processor(event)
# import foo as test
# o = test.TestClass()
# o.testMethod()
