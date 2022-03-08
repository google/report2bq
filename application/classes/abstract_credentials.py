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

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

import pytz
from dateutil.relativedelta import relativedelta
from google.auth.transport import requests
from google.oauth2 import credentials

from classes.abstract_datastore import AbstractDatastore
from classes.exceptions import CredentialsError


@dataclass
class ProjectCredentials(object):
  client_id: str
  client_secret: str


class AbstractCredentials(object):
  """Abstract Credentials.

  This is the Credentials contract to be fufilled by any concrete version. It
  contains the OAuth functions necessary for Credentials, but none of the ways
  to fetch the credential sources or to store updated ones. These should be
  done by the 'datastore', which will be created in the concrete implementation.

  'datastore' can return a 'pass', although if it is not set, this will cause
  failures further down the line if an attempt is made to store or load
  credentials.
  """
  _email: str = None
  _project: str = None

  def datastore(self) -> AbstractDatastore:
    """The datastore property."""
    pass

  def datastore(self, f: AbstractDatastore) -> None:
    """Sets the datastore property."""
    self._datastore = f

  def project_credentials(self) -> ProjectCredentials:
    """The project credentials."""
    pass

  def token_details(self) -> Dict[str, Any]:
    """The users's refresh and access token."""
    pass

  def _refresh_credentials(self, creds: credentials.Credentials) -> None:
    """Refreshes the Google OAuth credentials.

    Returns:
        google.oauth2.credentials.Credentials: the credentials
    """
    creds.refresh(requests.Request())
    self.credentials = creds

  def _to_utc(self, last_date: datetime) -> datetime:
    if (last_date.tzinfo is None or
            last_date.tzinfo.utcoffset(last_date) is None):
      last_date = pytz.UTC.localize(last_date)

    return last_date

  @property
  def credentials(self) -> credentials.Credentials:
    """Fetches the credentials.

    Returns:
       (google.oauth2.credentials.Credentials):  the credentials
    """
    expiry = self._to_utc(
        datetime.now().astimezone(pytz.utc) + relativedelta(minutes=30))
    if token := self.token_details:
      if token.get('access_token'):
        # This handles old-style credential storages.
        creds = credentials.Credentials.from_authorized_user_info({
            'token': token['access_token'],
            'refresh_token': token['refresh_token'],
            'client_id': self.project_credentials.client_id,
            'client_secret': self.project_credentials.client_secret,
        })

      else:
        creds = \
            credentials.Credentials.from_authorized_user_info(token)

      if creds.expired:
        creds.expiry = expiry
        self._refresh_credentials(creds=creds)

    else:
      creds = None
      raise CredentialsError(message='not found', email=self._email)

    return creds

  @property
  def auth_headers(self) -> Dict[str, Any]:
    """Returns authorized http headers.

    This function calls the 'get_credentials' to grab the latest, refreshed
    OAuth credentials for the user, and uses them to create the OAuth2 header
    dict needed for some HTTP requests.

    Returns:
      oauth2_headers (Dict[str, Any]):  the OAuth headers
    """
    oauth2_header = {}
    self.credentials.apply(oauth2_header)

    return oauth2_header

  @credentials.setter
  def credentials(self,
                  creds: credentials.Credentials) -> None:
    """Stores the credentials.

    This function uses the datastore to store the user credentials for later.
    It's default behaviour is 'pass' as it relies upon the concrete
    implementation's datastore which is the only one that should be aware of
    where the creds are being stored.

    Args:
        creds (credentials.Credentials): [description]
    """
    pass
