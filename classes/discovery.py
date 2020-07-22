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

Performs OAuth against the GMB and BigQuery REST APIs.

Both APIs require similar the same authentication flow. a credential file has
be available on Colossus (for security reasons) in a specific directory. It
is accessed via gfile and used to authenticate an https connection.
"""

import json
import logging

from typing import Any, Dict, List, Tuple, Mapping
from classes.services import Service

from apiclient import discovery
from oauth2client.client import AccessTokenCredentials
from classes.credentials import Credentials
from classes.decorators import timeit, measure_memory

class DiscoverService(object):
  @classmethod
  def get_unknown_service(cls, credentials: Credentials, **kwargs: Mapping[str, Any]):
    """Fetch a discoverable API service.

    If this is not the case (or, upon attempting to is the service an
    authorization error is raised) the service can be cleared and forced to
    reload.

    Args:

    Returns:
      service: a service for REST calls
    """
    credentials.get_credentials()
    _credentials = AccessTokenCredentials(credentials.token_details['access_token'], user_agent='report2bq')
    https = discovery.httplib2.Http()
    auth_https = _credentials.authorize(https)
    service = discovery.build(http=https, cache_discovery=False, **kwargs)
    return service


  @classmethod
  def get_service(cls, service: Service, credentials: Credentials, api_key: str=None) -> discovery.Resource:
    """Fetch a discoverable API service.

    If this is not the case (or, upon attempting to is the service an
    authorization error is raised) the service can be cleared and forced to
    reload.

    Args:

    Returns:
      service: a service for REST calls
    """
    definition = service.definition() 
    if definition:
      return cls.get_unknown_service(credentials=credentials, developerKey=api_key, **definition)

    else:
      raise Exception(f'Unknown service {service}') 

