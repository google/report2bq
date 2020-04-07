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

# Python Imports
import json
import logging

# Oauth Import
from google_auth_oauthlib import flow

# Class Imports
from absl import app
from absl import flags
from pprint import pprint
from typing import Dict, Any

from classes.cloud_storage import Cloud_Storage


FLAGS = flags.FLAGS
flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'GCP Project')

flags.DEFINE_string('client_id',
                    None,
                    'GCP Project client id')
flags.DEFINE_string('client_secret',
                    None,
                    'GCP Project client secret')

flags.DEFINE_Boolean('console', False,
                     'Show the URL in the console, instead of spawning a browser')

flags.DEFINE_boolean('use_cloud', False, 
                     'Use the secrets from the cloud project and save the result to cloud')

flags.mark_flag_as_required('email')
flags.mark_flag_as_required('project')


def main(unused_argv):
# Init App Flow
  appflow = flow.InstalledAppFlow.from_client_secrets_file(
      'config_files/client_secrets.json',
      scopes = [
          'https://www.googleapis.com/auth/adsdatahub', # ADH
          'https://www.googleapis.com/auth/doubleclickbidmanager', # DBM
          'https://www.googleapis.com/auth/dfareporting', # DCM Reporting
          'https://www.googleapis.com/auth/bigquery', # BigQuery
          'https://www.googleapis.com/auth/devstorage.read_write', # GCS
          'https://www.googleapis.com/auth/datastore', # Firestore
          'https://www.googleapis.com/auth/doubleclicksearch' # Firestore
      ]
  )

  credentials = appflow.run_local_server(
    host='localhost',
    port=8081,
    authorization_prompt_message='Please visit his URL to complete the authorization flow: {url}',
    success_message='Authorization complete: you may now close this window.',
    open_browser=True)

  # # Init App Flow local server
  # appflow.run_console()

  # # Credentials
  # credentials = appflow.credentials

  # Token Details
  token_details = {
    'access_token': credentials.token,
    'refresh_token': credentials.refresh_token
  }

  # Save Token Details to File
  token_file = open('config_files/user_token_details.json', 'w')
  token_file.write(json.dumps(token_details))
  token_file.close()


if __name__ == '__main__':
  app.run(main)

