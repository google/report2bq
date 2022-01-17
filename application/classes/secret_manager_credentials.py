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
from __future__ import annotations

import json

from google.oauth2 import credentials
from typing import Any, Dict

from classes import decorators
from classes.abstract_credentials import AbstractCredentials
from classes.abstract_datastore import AbstractDatastore
from classes.cloud_storage import Cloud_Storage
from classes.report_type import Type


class Credentials(AbstractCredentials):
  def __init__(self, email: str = None,
               project: str = None) -> Credentials:
    self._email = email
    self._project = project

  @property
  def datastore(self) -> AbstractDatastore:
    """The datastore property."""
    from classes.secret_manager import SecretManager
    return SecretManager(project=self._project, email=self._email)

  @property
  def project_credentials(self) -> Dict[str, Any]:
    """The project credentials."""
    return self.datastore.get_document(id='client_secret')

  @property
  def token_details(self) -> Dict[str, Any]:
    """The users's refresh and access token."""
    return self.datastore.get_document(id=self.encode_key(self._email))

  @property
  def bucket(self) -> str:
    """The GCS bucket containing credentials."""
    return f'{self._project}-report2bq-tokens'

  @property
  def client_token(self) -> str:
    """The name of the token file in GCS."""
    return f'{self._email}_user_token.json'

  def store_credentials(self, creds: credentials.Credentials) -> None:
    """Stores the credentials.

    This function uses the datastore to store the user credentials for later.

    Args:
        creds (credentials.Credentials): the user credentials."""
    if self._email:
      key = self.encode_key(self._email)
      data = {
          'access_token': creds.token,
          'refresh_token': creds.refresh_token,
          '_key': key
      }
      self.datastore.update_document(id=key, new_data=data)
