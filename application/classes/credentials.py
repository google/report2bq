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

import base64
import json

from google.oauth2 import credentials
from typing import Any, Dict

from classes import decorators
from classes.abstract_credentials import AbstractCredentials
from classes.abstract_datastore import AbstractDatastore
from classes.cloud_storage import Cloud_Storage
from classes.report_type import Type


class Credentials(AbstractCredentials):
  """Cloud connected credentials handler

  This extends and implements the AbstractCredentials for credentials held
  in Firestore or GCS on the cloud.
  """
  def __init__(self, email: str=None, project: str=None) -> Credentials:
      self._email=email
      self._project=project

  @decorators.lazy_property
  def datastore(self) -> AbstractDatastore:
    """The datastore property."""
    from classes.firestore import Firestore
    return Firestore()

  @decorators.lazy_property
  def project_credentials(self) -> Dict[str, Any]:
    """The project credentials.

    TODO: Remove the GCS check when fully migrated to Firestore."""
    return self.datastore.get_document(type=Type._ADMIN,
                                       id='auth', key='client_secret') or \
      json.loads(Cloud_Storage.fetch_file(bucket=self.bucket,
                                          file='client_secrets.json'))

  @property
  def token_details(self) -> Dict[str, Any]:
    """The users's refresh and access token."""
    # TODO: Remove the GCS check when fully migrated to Firestore.
    return self.datastore.get_document(type=Type._ADMIN, id='auth',
                                       key=self.key) or \
      json.loads(Cloud_Storage.fetch_file(bucket=self.bucket,
                                          file=self.client_token))

  @decorators.lazy_property
  def bucket(self) -> str:
    """The GCS bucket containing credentials."""
    # TODO: Remove when fully migrated to Firestore.
    return f'{self._project}-report2bq-tokens'

  @decorators.lazy_property
  def client_token(self) -> str:
    """The name of the token file in GCS."""
    # TODO: Remove when fully migrated to Firestore.
    return f'{self._email}_user_token.json'

  @decorators.lazy_property
  def key(self) -> str:
    """The key to use in Firestore

    Converts an email address to a base64 version to use as a key since
    Firestore can only have [A-Za-z0-9] in keys. Stripping the '=' padding is
    fine as the value will never have to be translated back.

    Returns:
        str: base64 representation of the key value.
    """
    _key = \
      base64.b64encode(self._email.encode('utf-8')).decode('utf-8').rstrip('=')
    return _key

  def store_credentials(self, creds: credentials.Credentials) -> None:
    """Stores the credentials.

    This function uses the datastore to store the user credentials for later.

    Args:
        creds (credentials.Credentials): the user credentials."""
    # TODO: Remove the GCS write when fully migrated to Firestore.
    refresh_token_details = {
      'access_token': creds.token,
      'refresh_token': creds.refresh_token
    }
    self.datastore.update_document(type=Type._ADMIN, id='auth',
                                   new_data={self.key: refresh_token_details})
    Cloud_Storage.write_file(
      bucket=self.bucket, file=self.client_token,
      data=json.dumps(refresh_token_details).encode('utf-8'))
    return creds
