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

import logging
import os
import traceback

from classes import credentials
from classes import discovery
from classes import firestore
from classes import report_type
from classes import services

from base64 import urlsafe_b64encode
from email.mime import text
from typing import Any, Dict, List


class GMailMessage(object):
  def __init__(self, to: List[str]=[], cc: List[str]=[], subject: str=None,
               body: str='', snippet: str=None,
               project: str=None) -> GMailMessage:
    self._to = to
    self._cc = cc
    self._subject = (
      f'[REPORT2BQ on {project or os.environ.get("GCP_PROJECT")}]: '
      f'{subject or "Important Report2BQ message"}')
    self._body = body
    self._snippet = snippet
    self._project = project or os.environ.get('GCP_PROJECT')

  def create_message(self) -> Dict[str, Any]:
    """Creates a gmail api format message.

    Returns:
        Dict[str, Any]: the message object.
    """
    message = text.MIMEText(self._body)
    message['to'] = ','.join(self._to)
    message['cc'] = ','.join(self._cc)
    message['from'] = \
      f'Report2BQ on {self._project} <noreply-report2bq@google.com>'
    message['subject'] = self._subject
    body = \
      {
        'raw':
        urlsafe_b64encode(message.as_string().encode('utf-8')).decode('utf-8')
      }

    return body


def send_message(message: GMailMessage,
                 credentials: credentials.Credentials,
                 **unused) -> None:
  """Sends a message via the Gmail API.

  Args:
      message (GMailMessage): the message.
      credentials (Credentials): credentials valid for the gmail scope.
  """
  gmail = discovery.get_service(service=services.Service.GMAIL,
                                credentials=credentials)
  request = gmail.users().messages().send(userId='me',
                                          body=message.create_message())
  response = request.execute()
  logging.info(response)

def error_to_trace(error: Exception=None) -> str:
  """Pulls a python stack trace from an error.

  Args:
      error (Exception, optional): the exception. Defaults to None.

  Returns:
      str: the stack trace
  """
  trace = ''
  if error:
    tb = traceback.TracebackException.from_exception(error).format()
    if tb:
      trace = '\n\nTrace:\n\n' + ''.join(tb)

  return f'{trace}'

def create_error_email(email: str,
                       product: str,
                       event: Dict[str, Any],
                       error: Exception) -> GMailMessage:
  """Creates an error email message.

  This emails any error messages to the job owner and the administrator (if
  defined). The error will be converted to a full Python stack trace if
  possible.

  Args:
      email (str): job owner email.
      product (str): product name.
      event (Dict[str, Any]): pubsub event.
      error (Exception): error to send.

  Returns:
      the GMailMessage with the error in.
  """
  body = (
      f'\nError: {error if error else "No exception."}\n\n'
      f'{error_to_trace(error)}'
      f'Event data: {event}'
  )
  if not (administrator := os.environ.get('ADMINISTRATOR_EMAIL')):
    if firestore_admin := \
      firestore.Firestore().get_document(type=report_type.Type._ADMIN,
                                       id='admin'):
      administrator = firestore_admin.get('email')
  cc = [administrator] if administrator else []

  message = GMailMessage(to=[email],
                         cc=cc,
                         subject=f'Error in {product or "Report2BQ"}',
                         project=os.environ.get('GCP_PROJECT'),
                         body=body)

  return message
