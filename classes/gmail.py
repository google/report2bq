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
import collections
import itertools
import json
import logging
import os
import pprint
import pytz
import time

from classes.credentials import Credentials
from classes import discovery
from classes.services import Service

from base64 import urlsafe_b64encode
from email.mime.text import MIMEText
from typing import Any, Dict, List



class GMailMessage(object):
  def __init__(self, to: List[str]=[], cc: List[str]=[], subject: str=None, body: str='', snippet: str=None, project: str=None):
    self._to = to
    self._cc = cc
    self._subject = f'[REPORT2BQ on {project or os.environ.get("GCP_PROJECT")}]: {subject or "Important Report2BQ message"}'
    self._body = body
    self._snippet = snippet
    self._project = project or os.environ.get('GCP_PROJECT')

  def create_message(self) -> Dict[str, Any]:
    message = MIMEText(self._body)
    message['to'] = ','.join(self._to)
    message['cc'] = ','.join(self._cc)
    message['from'] = f'Report2BQ on {self._project} <noreply-report2bq@google.com>'
    message['subject'] = self._subject
    body = {'raw': urlsafe_b64encode(message.as_string().encode('utf-8')).decode('utf-8')}

    return body


class GMail(object):

  def send_message(self, message: str, credentials: Credentials):
    gmail = discovery.get_service(service=Service.GMAIL,
                                  credentials=credentials)
    request = gmail.users().messages().send(userId='me', body=message.create_message())
    response = request.execute()

    logging.info(response)

