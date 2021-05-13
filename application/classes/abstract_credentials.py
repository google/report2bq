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

from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.auth.transport import requests
from google.oauth2 import credentials

from classes import decorators
from classes.abstract_datastore import AbstractDatastore
from typing import Any, Dict


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

  def datastore(self) -> AbstractDatastore:
    """The datastore property."""
    pass

  def datastore(self, f: AbstractDatastore) -> None:
    """Sets the datastore property."""
    self._datastore = f

  def project_credentials(self) -> Dict[str, Any]:
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
    self.store_credentials(creds)

  def get_credentials(self) -> credentials.Credentials:
    """Fetches the credentials.

    Returns:
       (google.oauth2.credentials.Credentials):  the credentials
    """
    secrets = \
      self.project_credentials.get('web') or \
        self.project_credentials.get('installed')

    expiry = datetime.now() + relativedelta(days=7)
    if self.token_details.get('access_token'):
      creds = credentials.Credentials.from_authorized_user_info({
        'token': self.token_details['access_token'],
        'refresh_token': self.token_details['refresh_token'],
        'client_id': secrets['client_id'],
        'client_secret': secrets['client_secret'],
        'expiry': expiry.strftime("%Y-%m-%dT%H:%M:%S"),
      })
      self._refresh_credentials(creds=creds)

    else:
      creds = \
        credentials.Credentials.from_authorized_user_info(self.token_details)
      if creds.expired:
        creds.expiry = expiry
        self._refresh_credentials(creds=creds)


    return creds

  def get_auth_headers(self) -> Dict[str, Any]:
    """Returns authorized http headers.

    This function calls the 'get_credentials' to grab the latest, refreshed
    OAuth credentials for the user, and uses them to create the OAuth2 header
    dict needed for some HTTP requests.

    Returns:
      oauth2_headers (Dict[str, Any]):  the OAuth headers
    """
    oauth2_header = {}
    self.get_credentials().apply(oauth2_header)

    return oauth2_header

  def store_credentials(self,
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

  def encode_key(self, key: str) -> str:
    """Creates the key to use in Firestore.

    Converts an string to a base64 version to use as a key since
    Firestore can only have [A-Za-z0-9] in keys. Stripping the '=' padding is
    fine as the value will never have to be translated back.

    Returns:
        str: base64 representation of the key value.
    """
    return base64.b64encode(key.encode('utf-8')).decode('utf-8').rstrip('=')
