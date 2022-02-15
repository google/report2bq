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

import dataclasses
import enum
from typing import Any, Mapping, Optional

import immutabledict

from classes import decorators


@dataclasses.dataclass
class ServiceDefinition(object):
  name: Optional[str] = None
  version: Optional[str] = None
  uri: Optional[str] = None

  @decorators.lazy_property
  def to_args(self) -> Mapping[str, Any]:
    """Return the service definition as keyword args.

    This is defined as lazy so it can be referred to as a
    property instead of a function which makes the code read
    cleaner.

    Returns:
        Mapping[str, Any]: the definition as kwargs
    """
    args = {}
    if self.name:
      args['serviceName'] = self.name
    if self.version:
      args['version'] = self.version
    if self.uri:
      args['discoveryServiceUrl'] = self.uri
    return args


@enum.unique
class Service(enum.Enum):
  """Service definitions.

  Defines the access points for the Google services used.
  """
  ADH = enum.auto()
  BQ = enum.auto()
  CM = enum.auto()
  DV360 = enum.auto()
  GA360 = enum.auto()
  GMAIL = enum.auto()
  SA360 = enum.auto()
  SCHEDULER = enum.auto()

  @decorators.lazy_property
  def definition(self) -> ServiceDefinition:
    """Fetch the ServiceDefinition.

    Lazily returns the dataclass containing the service definition
    details. It has to be lazy, as it can't be defined at
    initialization time.

    Returns:
        ServiceDefinition: the service definition
    """
    (name, version) = SERVICE_DEFINITIONS.get(self)
    return ServiceDefinition(
        name=name,
        version=version,
        uri=f'https://{name}.googleapis.com/$discovery/rest?version={version}')


SERVICE_DEFINITIONS = \
    immutabledict.immutabledict({
        Service.ADH: ('adsdatahub', 'v1'),
        Service.BQ: ('bigquery', 'v2'),
        Service.CM: ('dfareporting', 'v3.5'),
        Service.DV360: ('doubleclickbidmanager', 'v1.1'),
        Service.GA360: ('analyticsreporting', 'v4'),
        Service.GMAIL: ('gmail', 'v1'),
        Service.SA360: ('doubleclicksearch', 'v2'),
        Service.SCHEDULER: ('cloudscheduler', 'v1'),
    })
