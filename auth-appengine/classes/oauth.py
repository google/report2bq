# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from auth.datastore import secret_manager
from auth.credentials import Credentials
from flask import Flask, Request
from google.auth.transport.requests import Request
from oauth2client import client

logging.getLogger().setLevel(logging.INFO)

app = Flask(__name__)


class OAuth(object):
  # Scope definitions here:
  #   https://developers.google.com/identity/protocols/oauth2/scopes
  SCOPES = [
      'https://www.googleapis.com/auth/adsdatahub',
      'https://www.googleapis.com/auth/analytics',
      'https://www.googleapis.com/auth/bigquery',
      'https://www.googleapis.com/auth/cloud-platform',
      'https://www.googleapis.com/auth/datastore',
      'https://www.googleapis.com/auth/devstorage.read_write',
      'https://www.googleapis.com/auth/dfareporting',
      'https://www.googleapis.com/auth/doubleclickbidmanager',
      'https://www.googleapis.com/auth/doubleclicksearch',
      'https://www.googleapis.com/auth/gmail.send',
  ]
  project = os.environ['GOOGLE_CLOUD_PROJECT']

  def oauth_complete(self, request: Request):
    if (request.get_data()):
      app.logger.info(f'data:\n{request.get_data()}')
      auth_code = str(request.get_data(), encoding='utf-8')

    else:
      app.logger.error('No code sent!')
      return 'AUTH FAIL: No authentication code received.'

    project_credentials = Credentials(
        project=self.project,
        email=None, datastore=secret_manager.SecretManager).project_credentials

    credentials = client.credentials_from_code(
        client_id=project_credentials.client_id,
        client_secret=project_credentials.client_secret,
        scope=self.SCOPES,
        code=auth_code
    )

    email = credentials.id_token['email']
    cm = Credentials(project=self.project, email=email,
                     datastore=secret_manager.SecretManager)
    cm.store_credentials(creds=credentials)

    return 'Authenticated!'
