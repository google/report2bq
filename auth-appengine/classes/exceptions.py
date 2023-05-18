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

from auth.credentials_helpers import encode_key


class CredentialsError(Exception):
  def __init__(self, message: str = None,
               email: str = None) -> CredentialsError:
    self.message = message
    self.email = email

  def __repr__(self) -> str:
    response = ['Credentials error']

    if self.message:
      response.append(f'"{self.message}"')

    if self.email:
      response.append(
        f'handling {self.email} ({encode_key(self.email)})')

    return ' '.join(response)

  __str__=__repr__
