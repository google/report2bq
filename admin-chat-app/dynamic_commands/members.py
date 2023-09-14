# Copyright 2022 Google Inc. All Rights Reserved.
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
from __future__ import annotations

import json
from typing import Any, Dict, Mapping

from auth.credentials_helpers import encode_key
from auth import secret_manager
from googleapiclient import discovery
from httplib2 import Http
from service_framework import service_builder, services

from classes.message_generator import MessageGenerator
from dynamic import DynamicClass


class Processor(DynamicClass):
  def run(self, **kwargs: Mapping[str, str]) -> Dict[str, Any]:
    generator = MessageGenerator(kwargs['request_json'])

    members = self.chat.spaces().messages().members().list(
      parent = generator.request.message.space.name
    ).execute
    print(members)
    return members

  def chat(self) -> discovery.Resource:
    key = secret_manager.SecretManager(
        project=self.project).get_document(
        encode_key("service account"))
    return service_builder.build_service(
        service=services.Service.CHAT, key=key,
        extra_scopes=['https://www.googleapis.com/auth/chat.bot'])
