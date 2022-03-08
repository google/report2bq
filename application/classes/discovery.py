# Copyright 2020 Google Inc. All Rights Reserved.
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

__author__ = ['davidharcombe@google.com (David Harcombe)']

"""Discovery Class.

Authenticate and fetch a discoverable API service.
"""

from typing import Any, Mapping
from classes.services import Service

from apiclient import discovery
from oauth2client.client import AccessTokenCredentials
from classes.secret_manager_credentials import Credentials


def get_service(service: Service,
                credentials: Credentials,
                api_key: str=None) -> discovery.Resource:
  """Fetch a discoverable API service.

  Create an endpoint to one of the Google services listed in Services.py as
  a defined service. Only services listed in the Services enum can be used,
  and they each have a  in a ServiceDefinition containing all the information
  needed to create the service. These parameters are decomposed to a dict of
  keyword arguments ans passed on to the Google Discovery API.

  Not all services require an API key, hence it is optional.

  Args:
    service (Service): [description]
    credentials (Credentials): [description]
    api_key (str, optional): [description]. Defaults to None.

  Returns:
      discovery.Resource: a service for REST calls

  Raises:
      NotImplementedError: if an invalid service is requested.
  """
  if definition := service.definition:
    _credentials = \
      AccessTokenCredentials(credentials.credentials.token,
                             user_agent='report2bq')
    auth_https = _credentials.authorize(discovery.httplib2.Http())
    service = discovery.build(http=auth_https,
                              cache_discovery=False,
                              developerKey=api_key,
                              **definition.to_args)
    return service

  else:
    raise NotImplementedError(f'Unknown service {service}')

