#Copyright 2020 Google LLC
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
from __future__ import annotations

import json

import google.auth.transport.requests
import google.oauth2.credentials

from classes import decorators
from classes import files
from classes.cloud_storage import Cloud_Storage
from typing import Any, Dict


class Credentials(object):
  """Credentials handler
  """
  @decorators.lazy_property
  def bucket(self) -> str:
    return f'{self.project}-report2bq-tokens'

  @decorators.lazy_property
  def project_credentials(self) -> Dict[str, Any]:
    c = None
    if self.in_cloud:
      c = json.loads(Cloud_Storage.fetch_file(bucket=self.bucket,
                                              file='client_secrets.json'))
    else:
      with open(f'{files.get_file_path("tokens")}/client_secrets.json',
                'r') as token_file:
        c = json.loads(token_file.read())

    return c

  @property
  def client_token(self) -> str:
    return f'{self.email}_user_token.json'

  @property
  def token_details(self) -> Dict[str, Any]:
    c = None
    if self.in_cloud:
      c = json.loads(Cloud_Storage.fetch_file(bucket=self.bucket,
                                              file=self.client_token))
    else:
      with open(f'{files.get_file_path("tokens")}/{self.client_token}',
                'r') as token_file:
        c = json.loads(token_file.read())

    return c

  def __init__(self,
               in_cloud: bool=True,
               email: str=None,
               project: str=None) -> Credentials:
    """
    Initialize Credential Class
    """
    self.project = project
    self.email = email
    self.in_cloud = in_cloud

  def _refresh_credentials(self) -> google.oauth2.credentials.Credentials:
    secrets = \
      self.project_credentials.get('web') or \
        self.project_credentials.get('installed')

    creds = google.oauth2.credentials.Credentials(
        None,
        refresh_token = self.token_details['refresh_token'],
        token_uri = "https://accounts.google.com/o/oauth2/token",
        client_id = secrets['client_id'],
        client_secret = secrets['client_secret']
    )

    creds.refresh(google.auth.transport.requests.Request())
    refresh_token_details = {
      'access_token': creds.token,
      'refresh_token': creds.refresh_token
    }

    Cloud_Storage.write_file(
      bucket=self.bucket, file=self.client_token,
      data=json.dumps(refresh_token_details).encode('utf-8'))

    return creds

  def get_credentials(self) -> google.oauth2.credentials.Credentials:
    """
    Return credentials
    Returns:
       credential object
    """
    return self._refresh_credentials()

  def get_auth_headers(self) -> Dict[str, Any]:
    """
    Returns authorized http headers
    Returns:
      OAuth authenticated headers
    """
    oauth2_header = {}
    self._refresh_credentials().apply(oauth2_header)

    return oauth2_header
