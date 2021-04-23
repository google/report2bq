#Copyright 2021 Google LLC
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

import base64
import json
import logging

from absl import app
from absl import flags
from contextlib import suppress
from datetime import datetime

from classes.firestore import Firestore
from classes.report_type import Type


logging.basicConfig(
  filename=( 'firestore_upload-'
            f'{datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.log'),
  format='%(asctime)s %(message)s',
  datefmt='%Y-%m-%d %I:%M:%S %p',
  level=logging.DEBUG
)

FLAGS = flags.FLAGS
flags.DEFINE_string('key', None, 'Key to create/update')
flags.DEFINE_string('file', None, 'File containing json data')
flags.DEFINE_bool('encode_key', False, 'Encode the key (for tokens).')
flags.mark_flags_as_required(['file'])


def upload(key: str, file: str, encode_key: bool) -> None:
  def _encode() -> str:
    _key = \
      base64.b64encode(k.encode('utf-8')).decode('utf-8').rstrip('=')
    return _key

  data = None

  if file:
    with open(file, 'r') as data_file:
      src_data = json.loads(data_file.read())

  if encode_key:
    data = {}
    for (k, v) in src_data.items():
      v["_key"] = k
      data[_encode()] = v

  else:
    data = src_data

  f = Firestore()
  f.update_document(Type._ADMIN, id=key, new_data=data)

def main(unused_argv):
  event = {
    'key': FLAGS.key,
    'file': FLAGS.file,
    'encode_key': FLAGS.encode_key
  }
  upload(**event)


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
