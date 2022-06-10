# Copyright 2021 Google LLC
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

from google.cloud import service_usage
from google.api_core.exceptions import ResourceExhausted
from classes import decorators


class APIService(object):
  _project = None

  @property
  def project(self) -> str:
    return self._project

  @project.setter
  def project(self, project: str) -> None:
    self._project = project

  def __init__(self, project: str) -> APIService:
    self.project = project

  @decorators.retry(ResourceExhausted, tries=3, delay=30, backoff=1)
  def check_api(self, api: str) -> bool:
    # manager = service_usage.ServiceUsageClient()
    # services = manager.list_services(
    #     request=service_usage.ListServicesRequest(
    #         parent=f'projects/{self.project}',
    #         filter='state:ENABLED'))

    # return [service.config.name
    #         for service in services
    #         if service.config.name == f'{api}.googleapis.com'] != []

    return True
