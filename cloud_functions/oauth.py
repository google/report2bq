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
import io
import json
import logging
import os

# Oauth Import
from flask import Request, Response, redirect, url_for
from google_auth_oauthlib import flow
from google_auth_oauthlib import helpers
from google.auth.transport.requests import Request
from oauth2client.client import Storage
from urllib.parse import urlparse, urlunparse

# Classes
from classes.files import Files
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore


class OAuthStorage(Storage):
  credentials = None

  def locked_get(self):
    return self.credentials

  def locked_put(self, credentials):
    self.credentials = credentials
  
  def locked_delete(self):
    self.credentials = None


class OAuth(object):
  SCOPES = [
      'https://www.googleapis.com/auth/doubleclickbidmanager', # DBM
      'https://www.googleapis.com/auth/dfareporting',          # DCM
      'https://www.googleapis.com/auth/bigquery',              # BigQuery
      'https://www.googleapis.com/auth/devstorage.read_write', # GCS
      'https://www.googleapis.com/auth/datastore',             # Firestore
      'https://www.googleapis.com/auth/doubleclicksearch'      # SA360
  ]

  def oauth_init(self, request: Request, project: str, email: str):
    project_credentials = json.loads(Files.fetch_file(
      '{project}-report2bq-tokens'.format(project=project),
      'client_secrets.json'
    ), encoding='utf-8')

    _flow = flow.Flow.from_client_config(
      client_config=project_credentials, 
      scopes=self.SCOPES)

    _flow.redirect_uri = f"https://{os.environ.get('FUNCTION_REGION')}-{os.environ.get('GCP_PROJECT')}.cloudfunctions.net/OAuthComplete"

    authorization_url, state = _flow.authorization_url(
      access_type='offline',
      include_granted_scopes='true'
    )

    firestore = Firestore()
    firestore.store_oauth_state(state=state, email=email, project=project)

    return redirect(authorization_url)


  def oauth_complete(self, request: Request):
    logging.info(request.args)
    
    state = request.args.get('state', type=str)
    firestore = Firestore()
    email, project = firestore.get_oauth_state(state)

    project_credentials = json.loads(Files.fetch_file(
      '{project}-report2bq-tokens'.format(project=project),
      'client_secrets.json'
    ), encoding='utf-8')
    
    _flow = flow.Flow.from_client_config(
      client_config=project_credentials, 
      scopes=self.SCOPES)
    _flow.redirect_uri = f"https://{os.environ.get('FUNCTION_REGION')}-{os.environ.get('GCP_PROJECT')}.cloudfunctions.net/OAuthComplete"

    r = urlparse(request.url)
    auth_response = urlunparse(['https',r.netloc,r.path,r.params,r.query,r.fragment])
    _flow.fetch_token(authorization_response=auth_response)

    logging.info(_flow.credentials)

    token_details = {
      'access_token': _flow.credentials.token,
      'refresh_token': _flow.credentials.refresh_token
    }

    Cloud_Storage.write_file(
      '{project}-report2bq-tokens'.format(project=project),
      '{email}_user_token.json'.format(email=email),
      json.dumps(token_details).encode('utf-8'))

    firestore.delete_oauth_state(state=state)

    return 'Ok'