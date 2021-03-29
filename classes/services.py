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

import enum
from classes import decorators
from typing import Any, Dict, Optional, Mapping

import dataclasses
import immutabledict


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
    if self.name: args['serviceName'] = self.name
    if self.version: args['version'] = self.version
    if self.uri: args['discoveryServiceUrl'] = self.uri
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
    return SERVICE_DEFINITIONS.get(self)


SERVICE_DEFINITIONS = \
  immutabledict.immutabledict({
    Service.ADH:
      ServiceDefinition(name='AdsDataHub', version='v1'),
    Service.BQ:
      ServiceDefinition(version='v2', name='bigquery'),
    Service.CM:
      ServiceDefinition(name='dfareporting', version='v3.3'),
    Service.DV360:
      ServiceDefinition(name='doubleclickbidmanager', version='v1.1'),
    Service.GMAIL:
      ServiceDefinition(name='gmail', version='v1'),
    Service.SA360:
      ServiceDefinition(name='doubleclicksearch', version='v2'),
    Service.SCHEDULER:
      ServiceDefinition(name='cloudscheduler', version='v1'),
  })
