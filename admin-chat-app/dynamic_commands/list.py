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

from httplib2 import Http

from classes.message_generator import MessageGenerator
from dynamic import DynamicClass


class Processor(DynamicClass):
  def run(self, **kwargs: Mapping[str, str]) -> Dict[str, Any]:
    message_generator = MessageGenerator(kwargs['request_json'])

    http = Http()
    resp, content = http.request(
        uri=kwargs['job_manager_uri'],
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8'},
        body=json.dumps({
            "message": {
                "action": "list",
                "email": kwargs['request'].user.email
            }}),
    )
    print(resp)
    print(content)

    list_response = json.loads(content)
    job_list = message_generator.job_list(list_response.get('response'))
    print(job_list)
    return job_list
