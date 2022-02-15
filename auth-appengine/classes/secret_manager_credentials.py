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

from typing import Any, Dict, Mapping, Union

from google.oauth2 import credentials

from classes.abstract_credentials import (AbstractCredentials,
                                          ProjectCredentials)
from classes.abstract_datastore import AbstractDatastore
from classes.decorators import lazy_property


class Credentials(AbstractCredentials):
  def __init__(self, email: str = None,
               project: str = None) -> Credentials:
    self._email = email
    self._project = project

  @lazy_property
  def datastore(self) -> AbstractDatastore:
    """The datastore property."""
    from classes.secret_manager import SecretManager
    return SecretManager(project=self._project, email=self._email)

  @lazy_property
  def project_credentials(self) -> ProjectCredentials:
    """The project credentials."""
    creds = None
    if _id := self.datastore.get_document(id='client_id'):
      secrets = {**_id, **self.datastore.get_document(id='client_secret')}
      # creds = ProjectCredentials(client_id=client_id,
      #                           client_secret=client_secret)
    else:
      if client_secret := self.datastore.get_document(id='client_secret'):
        secrets = \
            client_secret.get('web') or \
            client_secret.get('installed')

    creds = ProjectCredentials(client_id=secrets['client_id'],
                               client_secret=secrets['client_secret'])
    return creds

  @lazy_property
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

  def store_credentials(
          self,
          creds: Union[credentials.Credentials, Mapping[str, Any]]) -> None:
    """Stores the credentials.

    This function uses the datastore to store the user credentials for later.

    Args:
        creds (credentials.Credentials): the user credentials."""
    if self._email:
      key = self.encode_key(self._email)
      if not isinstance(creds, Mapping):
        try:
          token = creds.access_token
        except AttributeError:
          token = creds.token

        data = {
            'access_token': token,
            'refresh_token': creds.refresh_token,
            'email': self._email,
        }
      else:
        data = creds

      self.datastore.update_document(id=key, new_data=data)
