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

from classes.credentials import Credentials
from classes.gmail import GMail, GMailMessage

from absl import app
from absl import flags
from contextlib import suppress

FLAGS = flags.FLAGS
flags.DEFINE_string('email',
                     None,
                     'Report owner/user email')
flags.DEFINE_string('project',
                     None,
                     'GCP Project')

def main(unused_argv):
  message = GMailMessage(
    to=[FLAGS.email], 
    body='This is a test report2bq message', 
    project='galvanic-card-234919')

  mailer = GMail()
  mailer.send_message(message=message, credentials=Credentials(email=FLAGS.email, project=FLAGS.project))


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
