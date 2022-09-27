# Copyright 2020 Google LLC
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

from absl import app, flags
from classes.api_checker import APIService
from google.cloud import service_usage

import gcsfs
import json
import os


def main(unused) -> None:
  del unused
  fs = gcsfs.GCSFileSystem(project=os.environ.get('GCP_PROJECT'))
  with fs.open(
      ('report2bq-zz9-plural-z-alpha-report2bq-tokens/'
       'report2bq@report2bq-zz9-plural-z-alpha.iam.gserviceaccount.com.json'),
          'r') as auth_file:
    key_json = auth_file.read()
    if key_json:
      key = json.loads(key_json)

    print(key)


if __name__ == '__main__':
  app.run(main)
