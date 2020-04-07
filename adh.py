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
import json
import logging
import pprint
import re

# Class Imports
from absl import app
from absl import flags

from google_auth_oauthlib import flow
from googleapiclient.discovery import build
from classes.adh import ADH
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore
from classes.credentials import Credentials
from classes.report_type import Type


FLAGS = flags.FLAGS
flags.DEFINE_string('adh_customer',
                    None,
                    'ADH customer id')
flags.DEFINE_string('adh_query',
                    None,
                    'ADH query id')
flags.DEFINE_string('api_key',
                    None,
                    'ADH Developer Key')
flags.DEFINE_integer('days',
                     60,
                     'Number of days lookback, default is 60')
flags.DEFINE_string('dest_project',
                    None,
                    'Destination project for ADH results')
flags.DEFINE_string('dest_dataset',
                    None,
                    'Destination dataset for ADH results')

flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'GCP Project')

def _sanitize_string(original: str):
  return re.sub('[^a-zA-Z0-9,]', '_', original)

def main(unused_argv: list):
  adh = ADH(
    email=FLAGS.email, 
    project=FLAGS.project,
    adh_customer=FLAGS.adh_customer,
    adh_query=FLAGS.adh_query,
    api_key=FLAGS.api_key,
    days=FLAGS.days,
    dest_project=FLAGS.dest_project,
    dest_dataset=FLAGS.dest_dataset
  )
  adh.run()

if __name__ == '__main__':
  app.run(main)
