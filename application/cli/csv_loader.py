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

import logging

from absl import app
from absl import flags
from contextlib import suppress
from datetime import datetime

from main import report_upload

logging.basicConfig(
  filename=f'report_upload-{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log',
  format='%(asctime)s %(message)s',
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.DEBUG
)

FLAGS = flags.FLAGS
flags.DEFINE_string('name', None, 'filename')

flags.DEFINE_string('bucket', None, 'bucket')


# Stub main()
def main(unused_argv):
  event = {
    'name': FLAGS.name,
    'bucket': FLAGS.bucket,
  }
  report_upload(event, None)


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
