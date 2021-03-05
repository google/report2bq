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
import google.auth.transport.requests
import google.oauth2.credentials

# Class Imports
from classes.files import Files
from classes.cloud_storage import Cloud_Storage
from google.cloud import storage
from typing import Any, Dict

class Credentials(object):
  """Credentials handler
  """
  creds = None

  def __init__(self, in_cloud: bool=True, email: str=None, project: str=None):
    """
    Initialize Credential Class
    """
    self.project = project
    self.email = email
    self.bucket = f'{project}-report2bq-tokens'
    self.client_token = f'{email}_user_token.json'
    self.project_credentials = json.loads(
      Cloud_Storage.fetch_file(bucket=self.bucket, file='client_secrets.json'))
    self.token_details = json.loads(
      Cloud_Storage.fetch_file(bucket=self.bucket, file=self.client_token))


  def _refresh_credentials(
    self,
    project_credentials: Dict[str, str],
    user_token: Dict[str, str]
  ) -> Dict[str, str]:
    # Remove top-level element
    secrets = project_credentials['web'] \
      if 'web' in project_credentials else project_credentials['installed']

    # Init credentials
    creds = google.oauth2.credentials.Credentials(
        None,
        refresh_token = user_token['refresh_token'],
        token_uri = "https://accounts.google.com/o/oauth2/token",
        client_id = secrets['client_id'],
        client_secret = secrets['client_secret']
    )

    # Force Refresh token
    creds.refresh(google.auth.transport.requests.Request())
    refresh_token_details = {
      'access_token': creds.token,
      'refresh_token': creds.refresh_token
    }

    Cloud_Storage.write_file(
      bucket=self.bucket, file=self.client_token,
      data=json.dumps(refresh_token_details).encode('utf-8'))

    # logging.info(
    #   'Credentials are %s', f'{"valid" if creds.valid else "invalid"}')
    return creds


  def get_credentials(self):
    """
    Return credentials
    Returns:
       credential object
    """

    # Return
    return self._refresh_credentials(
      self.project_credentials, self.token_details)


  def get_auth_headers(self):
    """
    Returns authorized http headers
    Returns:
      OAuth authenticated headers
    """

    # Oauth Headers
    oauth2_header = {}

    # Apply credential to headers
    self._refresh_credentials(
      self.project_credentials, self.token_details).apply(oauth2_header)

    # Return authorized http transport
    return oauth2_header
