"""
Copyright 2018 Google LLC

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

# Oauth Import
from google_auth_oauthlib import flow

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

# Init App Flow local server
appflow.run_console()

# Credentials
credentials = appflow.credentials

# Token Details
token_details = {
  'access_token': credentials.token,
  'refresh_token': credentials.refresh_token
}

# Save Token Details to File
token_file = open('config_files/user_token_details.json', 'w')
token_file.write(json.dumps(token_details))
token_file.close()



