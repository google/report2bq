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

# Class Imports
from pprint import pprint

from classes.cloud_storage import Cloud_Storage

from oauth2client.client import OAuth2WebServerFlow

# Simple HTTP Handler - http://webpy.org/
import web

# URL Handler
urls = (
    '/', 'Index',
    '/oauth-response/', 'Consented',
)

with open('config_files/client_secrets.json', 'r') as secret_file:
  secrets = json.loads(secret_file.read())
  flow = OAuth2WebServerFlow(
      client_id=secrets['web']['client_id'],
      client_secret=secrets['web']['client_secret'],
      scope=[
        'https://www.googleapis.com/auth/adsdatahub', # ADH
        'https://www.googleapis.com/auth/bigquery', # BigQuery
        'https://www.googleapis.com/auth/cloud-platform', # Cloud Platform
        'https://www.googleapis.com/auth/datastore', # Firestore
        'https://www.googleapis.com/auth/dfareporting', # DCM Reporting
        'https://www.googleapis.com/auth/devstorage.read_write', # GCS
        'https://www.googleapis.com/auth/doubleclickbidmanager', # DBM
        'https://www.googleapis.com/auth/doubleclicksearch', # Firestore
      ],
      redirect_uri='http://localhost:8080/oauth-response/'
  )

class Index(object):
  """
  Handles response to user request for root page.
  """

  def GET(self):

    # Create redirect url
    auth_url = flow.step1_get_authorize_url()

    # Redirect to URL
    web.seeother(auth_url)


class Consented(object):
  """
  Handles response for /consent, redirects to oauth2.
  """

  def GET(self):
    # Response code
    response_code = web.input().code

    # Get credentials
    credentials = flow.step2_exchange(response_code)

    # JSON Token
    json_token = credentials.to_json()

    # Save File
    with open(f'config_files/user_token.json', 'w') as token_file:
      token_file.write(json_token)

    # Message
    message = 'Your JSON Token has been stored in config_files/token.json'

    return message


# web.py boiler plate
if __name__ == '__main__':
  app = web.application(urls, globals())
  app.run()
