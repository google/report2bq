# Copyright 2022 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging as log
import os
from typing import Any, Mapping, Union
from urllib.parse import urlparse, urlunparse

import flask
from auth.credentials import Credentials
from auth.datastore.secret_manager import SecretManager
from google.auth.transport import requests
from google.cloud import logging
from google.oauth2 import id_token
from google_auth_oauthlib import flow

from classes.report2bq import Report2BQ

logging_client = logging.Client()
logging_client.setup_logging()


def report2bq_admin(req: Union[Mapping[str, Any], flask.Request]):
  if isinstance(req, Mapping):
    request_json = req

  else:
    if req.method == 'GET':
      return 'Sorry, this function must be called from a Google Chat.'

    request_json = req.get_json(silent=True)

  print(request_json)

  return Report2BQ().process(req=request_json)


#
# OAuth functions
#
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
    # Auto added for some reason?
    'https://www.googleapis.com/auth/chat',
    'https://www.googleapis.com/auth/chat.spaces',
    'https://www.googleapis.com/auth/userinfo.email',
    'https://www.googleapis.com/auth/userinfo.profile',
    'openid',
]
REDIRECT_URI = 'https://us-central1-chats-zz9-plural-z-alpha.cloudfunctions.net/report2bq-oauth-complete'


def start_oauth(request: Union[Mapping[str, Any], flask.Request]) -> flask.Response:
  """Begins the oauth flow to authorize access to profile data."""
  client_secrets = SecretManager(
    project=os.environ.get('GCP_PROJECT')).get_document('client_secrets')
  oauth2_flow = flow.Flow.from_client_config(client_config=client_secrets,
                                             scopes=SCOPES,
                                             redirect_uri=REDIRECT_URI)

  oauth2_url, state = oauth2_flow.authorization_url(
      access_type='offline',
      include_granted_scopes='true')

  return flask.redirect(oauth2_url)


def complete_oauth(request: Union[Mapping[str, Any], flask.Request]) -> flask.Response:
  """Handles the OAuth callback."""
  log.info(request.values)

  client_secrets = SecretManager(project=os.environ.get(
      'GCP_PROJECT')).get_document('client_secrets')
  oauth2_flow = flow.Flow.from_client_config(client_config=client_secrets,
                                             scopes=SCOPES)

  oauth2_flow.redirect_uri = REDIRECT_URI

  result = urlparse(flask.request.url)
  auth_response = urlunparse(result._replace(scheme='https'))

  token = oauth2_flow.fetch_token(authorization_response=auth_response)
  id = id_token.verify_oauth2_token(
      id_token=token['id_token'], request=requests.Request())

  creds = oauth2_flow.credentials

  cm = Credentials(project=os.environ.get('GCP_PROJECT'),
                   email=id['email'],
                   datastore=SecretManager)
  c = _credentials_to_dict(creds)
  cm.store_credentials(creds=c)

  return 'Authenticated! You may close this window.'


def _credentials_to_dict(credentials):
  return {'token': credentials.token,
          'refresh_token': credentials.refresh_token,
          'token_uri': credentials.token_uri,
          'client_id': credentials.client_id,
          'client_secret': credentials.client_secret,
          'scopes': credentials.scopes}
