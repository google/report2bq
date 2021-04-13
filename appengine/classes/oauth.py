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
import io
import json
import logging
import os

# Oauth Import
from flask import Flask, Request, Response, redirect, url_for
from google.cloud import storage
from google_auth_oauthlib import flow
from google_auth_oauthlib import helpers
from google.auth.transport.requests import Request
from oauth2client import client
from urllib.parse import urlparse, urlunparse
from urllib.parse import unquote_plus as unquote

logging.getLogger().setLevel(logging.INFO)

app = Flask(__name__)

class OAuth(object):
  # Scope definitions here:
  #   https://developers.google.com/identity/protocols/oauth2/scopes
  SCOPES = [
    'https://www.googleapis.com/auth/analytics',
    'https://www.googleapis.com/auth/adsdatahub',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/datastore',
    'https://www.googleapis.com/auth/dfareporting',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/doubleclickbidmanager',
    'https://www.googleapis.com/auth/doubleclicksearch',
    'https://www.googleapis.com/auth/gmail.send',
  ]
  project = os.environ['GOOGLE_CLOUD_PROJECT']
  bucket = f'{project}-report2bq-tokens'

  def oauth_complete(self, request: Request):
    if(request.get_data()):
      app.logger.info(f'data:\n{request.get_data()}')
      auth_code = str(request.get_data(), encoding='utf-8')

    else:
      app.logger.error('No code sent!')
      return 'AUTH FAIL: No authentication code received.'

    project_credentials = json.loads(OAuth.fetch_file(
      self.bucket,
      'client_secrets.json'
    ), encoding='utf-8')

    credentials = client.credentials_from_code(
      client_id=project_credentials['web']['client_id'],
      client_secret=project_credentials['web']['client_secret'],
      scope=self.SCOPES,
      code=auth_code
    )

    email = credentials.id_token['email']

    token_details = {
      'access_token': credentials.access_token,
      'refresh_token': credentials.refresh_token
    }

    OAuth.write_file(
      self.bucket,
      f'{email}_user_token.json',
      json.dumps(token_details).encode('utf-8'))

    return 'Authenticated!'

  @staticmethod
  def fetch_file(bucket: str, file: str, credentials=None) -> str:
    """fetch a file from GCS

    Arguments:
      bucket {str} -- bucket name
      file {str} -- file name

    Returns:
      {str} -- file content
    """
    client = storage.Client(credentials=(credentials.get_credentials() if credentials else None))

    # logging.info('Fetching {f} from GCS'.format(f=file))

    try:
      content = client.get_bucket(bucket).blob(file).download_as_string()
    except Exception as ex:
      content = None
      logging.error('Error fetching file {f}\n{e}'.format(f=file, e=ex))

    return content


  @staticmethod
  def write_file(bucket: str, file: str, data: bytes, credentials=None) -> None:
    client = storage.Client(credentials=(credentials.get_credentials() if credentials else None))

    # logging.info(f'Writing {file} to GCS: {len(data)}')

    try:
      client.get_bucket(bucket).blob(file).upload_from_string(data)
    except Exception as ex:
      content = None
      logging.error(f'Error writing file {file}\n{ex}')
