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

import base64


def encode_key(key: str) -> str:
  """Creates the key to use in json oauth storage.

  Converts an string to a base64 version to use as a key since
  Firestore can only have [A-Za-z0-9] in keys. Stripping the '=' padding is
  fine as the value will never have to be translated back.

  Returns:
      str: base64 representation of the key value.
  """
  return \
      base64.b64encode(key.encode('utf-8')).decode('utf-8').rstrip('=') \
      if key else None
