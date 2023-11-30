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

import dynamic
import json
import os
from contextlib import suppress
from importlib import import_module
from typing import Any, Dict, Mapping

from absl import app
from auth import secret_manager, credentials
from classes.decorators import lazy_property
from googleapiclient import discovery
from httplib2 import Http
from service_framework import service_builder, services
from stringcase import snakecase

from classes import DictObj, error_to_trace
from classes.message_generator import MessageGenerator


class Report2BQ(object):
  @lazy_property
  def request(self) -> DictObj:
    return self._request

  @request.setter
  def request(self, request: Dict[str, Any]) -> None:
    self._request = DictObj(request)

  @lazy_property
  def project(self) -> str:
    return os.environ.get('GCP_PROJECT')

  @lazy_property
  def job_manager_uri(self) -> str:
    return os.environ.get('JOB_MANAGER_URI')

  @lazy_property
  def chat_service(self) -> discovery.Resource:
    creds = credentials.Credentials(email='david@anothercorp.net',
                                    project='chats-zz9-plural-z-alpha',
                                    datastore=secret_manager.SecretManager)
    return service_builder.build_service(
        service=services.Service.CHAT, key=creds.credentials,
        extra_scopes=['https://www.googleapis.com/auth/chat',
                      'https://www.googleapis.com/auth/chat.spaces'])

  def __init__(self) -> Report2BQ:
    pass

  def execute_dynamic_command(self,
                              command: str,
                              request_json: Mapping[str, Any]) -> Mapping[str, Any]:
    """execute_dynamic_command _summary_

    Args:
        command (str): _description_
        request_json (Mapping[str, Any]): _description_

    Returns:
        Mapping[str, Any]: _description_
    """
    try:
      processor = dynamic.DynamicClass.install(module_name=command,
                                               class_name='Processor',
                                               storage=dynamic.CloudStorage,
                                               bucket='my-gcs-bucket')
      attributes = {
          'request_json': request_json,
          'request': self.request,
          'job_manager_uri': self.job_manager_uri,
          'service': self.chat_service
      }
      output = processor.run(**attributes)
    except Exception as e:
      print(f'Exception in command processor: {error_to_trace(e)}')
      output = self.message_generator.error(
          command=command)

    return output

  def process(self, req: Mapping[str, Any]) -> Mapping[str, Any]:
    self.request = req
    self.message_generator = MessageGenerator(req)
    print(f'Message received: {self.request}')

    try:
      output = dict()

      match req.get('type'):
        case 'MESSAGE':
          if req.get('message').get('slashCommand'):
            # A recognized slash command. These must be defined in the UI.
            # Unknown slash commands ("/hello" for example) would be treated
            # as random text, and handled by one of the below cases.p
            match self.request.message.annotations[0].slashCommand.commandName:
              case '/list':
                output = self.job_list()

              case '/create':
                output = self.fetch_new_job_details()

              case '/edit':
                output = self.edit_job()

              case '/setup':
                output = self.setup()

              case '/authenticate':
                output = self.authenticate()

          elif self.request.message.annotations \
                  and self.request.message.annotations[0].userMention:
            # This is a user mention ("@[Bot name] [something something])
            match ' '.join(self.request.message.text.split(' ')[1:]):
              case 'create':
                output = self.fetch_new_job_details()

              case _ as name:
                app_name = f'@{self.request.message.annotations[0].userMention.user.displayName}'
                command_text = str(
                    self.request.message.text).replace(app_name, '')
                command = snakecase(command_text.strip()).lower()
                output = self.execute_dynamic_command(
                    command, req)

          elif self.request.message.space.type == 'DM':
            # This is just random text in a DM with the bot. Anything that's
            # a 'new' slash command (like '/hello', for example) will have the
            # '/' stripped and then be treated as just random text.
            text = ''.join(self.request.message.text.split('/')[1:]) \
                if self.request.message.text[0] == '/' \
                else self.request.message.text
            command = snakecase(text).lower()
            print(f'Loading and running {text}')
            output = self.execute_dynamic_command(command, req)

          else:
            # Anything else? IDK, raise an error.
            output = self.message_generator.error(
                message='Unsupported action {type}', type=req.get('type'))

        case 'CARD_CLICKED':
          if f := getattr(self,
                          self.request.action.actionMethodName,
                          None):
            output = f()

          else:
            output = self.message_generator.error(
                self.request.action.actionMethodName)

        case 'ADDED_TO_SPACE':
          return 'OK'

        case _ as unknown:
          output = self.message_generator.error(
              message='Unsupported action {type}', type=unknown)

      if output:
        print(output)

      return output

    except Exception as e:
      return {'text': error_to_trace(e)}

  def setup(self) -> Dict[str, Any]:
    space = {
        "space": {
            "spaceType": 'DIRECT_MESSAGE',
            "singleUserBotDm": True,
        }
    }

    result = self.chat_service.spaces().setup(body=space).execute()
    print(result)
    return {'text': 'Ok'}

  def job_list(self) -> Mapping[str, Any]:

    http = Http()
    resp, content = http.request(
        uri=self.job_manager_uri,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8'},
        body=json.dumps({
            "message": {
                "action": "list",
                "email": self.request.user.email
            }}),
    )
    print(resp)
    print(content)

    list_response = json.loads(content)
    job_list = self.message_generator.job_list(list_response.get('response'))
    print(job_list)
    return job_list

  def fetch_job(self, job_id: str = None) -> Mapping[str, Any]:
    (success, job) = self.fetch_job_details(job_id=job_id)

    if success:
      print(job)
      return self.message_generator.fetch_job(job=job)
    else:
      return self.message_generator.error(message='{job_id} not found.',
                                          job_id=job_id)

  def fetch_job_details(self, job_id: str = None) -> Mapping[str, Any]:
    if not job_id:
      job_id = self.request.common.parameters.job_id

    http = Http()
    resp, content = http.request(
        uri=self.job_manager_uri,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8'},
        body=json.dumps({
            "message": {
                "action": "get",
                "email": self.request.user.email,
                "job_id": job_id.split('/')[-1],
            }}),
    )
    list_response = json.loads(content)

    return list_response['response']

  def fetch_new_job_details(self) -> Mapping[str, Any]:
    return self.message_generator.create_new_job()

  def edit_job(self) -> Mapping[str, Any]:
    self.message_generator.edit = True
    if 'argumentText' in self.request.message:
      job_id = self.request.message.argumentText.strip()
    else:
      job_id = None

    (success, job) = self.fetch_job_details(job_id=job_id)

    if success:
      self.message_generator.job_data = job
      body = self.new_job_details(edit=True)
      print(body)
      return body

    else:
      return self.message_generator.error(
          message='{job_id} not found.',
          job_id=self.request.common.parameters.job_id)

  def new_job_details(self, edit: bool = False) -> Mapping[str, Any]:
    try:
      if edit:
        product = self.message_generator.job_data.pubsub_target.attributes.type
      else:
        inputs = self.request.common.formInputs
        product = inputs.product.stringInputs.value[0]

      match product:
        case 'dv360':
          return self.message_generator.job_details_dv360()
        case 'cm':
          return self.message_generator.job_details_cm360()
        case 'sa360':
          return self.message_generator.job_details_sa360()
      # case 'ga360':
      #   return self.message_generator.job_details_ga360()
      # case 'adh':
      #   return self.message_generator.job_details_adh()
        case _:
          return self.message_generator.error(
              '{product} is not a valid product or is not yet supported.',
              product=product)
    except Exception as e:
      print(error_to_trace(e))
      return self.message_generator.error(f'{error_to_trace(e)}',
                                          product=product)

  def create_job(self) -> Mapping[str, Any]:
    inputs = self.request.common.formInputs

    project = inputs.prior.stringInputs.value[0]
    email = inputs.prior.stringInputs.value[1]
    product = inputs.prior.stringInputs.value[2]

    parameters = {
        'action': 'create',
        'email': email,
        'project': project,
        'description': inputs.description.stringInputs.value[0],
    }

    if hasattr(inputs, 'options'):
      if options := inputs.options.stringInputs.value:
        for option in options:
          parameters[option] = True

    if dest_dataset := inputs.dest_dataset.stringInputs.value[0]:
      parameters['dest_dataset'] = dest_dataset

    if dest_project := inputs.dest_project.stringInputs.value[0]:
      parameters['dest_project'] = dest_project

    if dest_table := inputs.dest_table.stringInputs.value[0]:
      parameters['dest_table'] = dest_table

    if notify_message := inputs.notify_message.stringInputs.value[0]:
      parameters['notify_message'] = notify_message

    if minute := inputs.minute.stringInputs.value[0]:
      parameters['minute'] = minute

    if hour := inputs.hour.stringInputs.value[0]:
      parameters['hour'] = hour

    # Create parameter block for create
    match product:
      case 'dv360':
        parameters['runner'] = (inputs.runner.stringInputs.value[0] == 'runner')
        parameters['report_id'] = inputs.report_id.stringInputs.value[0]

      case 'cm':
        parameters['report_id'] = inputs.report_id.stringInputs.value[0]
        parameters['runner'] = (inputs.runner.stringInputs.value[0] == 'runner')
        parameters['profile'] = inputs.profile.stringInputs.value[0]

      case 'ga360':
        pass

      case 'sa360':
        if report_id := inputs.report_id.stringInputs.value[0]:
          parameters['report_id'] = report_id
          parameters['type'] = 'sa360_report'
        else:
          parameters['sa360_url'] = inputs.sa360_url.stringInputs.value[0]

    try:
      http = Http()
      resp, content = http.request(
          uri=self.job_manager_uri,
          method='POST',
          headers={'Content-Type': 'application/json; charset=UTF-8'},
          body=json.dumps({'message': parameters}),
      )
      print(content)
      _response = json.loads(content)

      print(_response)

      if response := _response.get('response'):
        success, job = response[0], response[1]
        if success:
          body = self.fetch_job(job_id=job['name'])

        else:
          body = self.message_generator.error(message='Job creation failed')

      self.chat_service.spaces().messages().create(
          parent=self.request.space.name,
          body=body
      ).execute()

    except Exception as e:
      print(error_to_trace(e))

    return 'OK'

  def disable_job(self) -> Mapping[str, Any]:
    return self._toggle_job(enable=False)

  def enable_job(self) -> Mapping[str, Any]:
    return self._toggle_job(enable=True)

  def _toggle_job(self, enable: bool) -> Any:
    job_id = self.request.common.parameters.job_id.split('/')[-1]

    http = Http()
    resp, content = http.request(
        uri=self.job_manager_uri,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8'},
        body=json.dumps({
            "message": {
                "action": f'{"en" if enable else "dis"}able',
                "job_id": job_id,
                "email": self.request.user.email,
            }}),
    )

    result = json.loads(content)

    if result['response'] == 'OK':
      job_detail = self.fetch_job(job_id=job_id)
      print(f'Message id: {self.request.message.name}')
      print(f'Message: {job_detail}')
    return 'OK'

  def delete_job(self) -> Any:
    job_id = self.request.common.parameters.job_id.split('/')[-1]

    http = Http()
    resp, content = http.request(
        uri=self.job_manager_uri,
        method='POST',
        headers={'Content-Type': 'application/json; charset=UTF-8'},
        body=json.dumps({
            "message": {
                "action": 'delete',
                "job_id": job_id,
                "email": self.request.user.email,
            }}),
    )

    result = json.loads(content)
    if result['response'] == 'OK':
      self.chat_service.spaces().messages().delete(
          name=self.request.message.name
      ).execute()

    return 'OK'

  def update_job(self) -> Mapping[str, Any]:
    inputs = self.request.common.formInputs

    project = inputs.prior.stringInputs.value[0]
    email = inputs.prior.stringInputs.value[1]
    product = inputs.prior.stringInputs.value[2]

    parameters = {
        'action': 'update',
        'email': email,
        'project': project,
        'description': inputs.description.stringInputs.value[0],
    }

    if (options := inputs.options) and (option_values := options.stringInputs.value):
      for option in option_values:
        if option != 'None':
          parameters[option] = True

    if dest_dataset := inputs.dest_dataset.stringInputs.value[0]:
      parameters['dest_dataset'] = dest_dataset

    if dest_project := inputs.dest_project.stringInputs.value[0]:
      parameters['dest_project'] = dest_project

    if dest_table := inputs.dest_table.stringInputs.value[0]:
      parameters['dest_table'] = dest_table

    if notify_message := inputs.notify_message.stringInputs.value[0]:
      parameters['notify_message'] = notify_message

    if minute := inputs.minute.stringInputs.value[0]:
      parameters['minute'] = minute

    if hour := inputs.hour.stringInputs.value[0]:
      parameters['hour'] = hour

    # Create parameter block for create
    match product:
      case 'dv360':
        parameters['runner'] = (inputs.runner.stringInputs.value[0] == 'runner')
        parameters['report_id'] = inputs.report_id.stringInputs.value[0]

      case 'cm':
        parameters['report_id'] = inputs.report_id.stringInputs.value[0]
        parameters['runner'] = (inputs.runner.stringInputs.value[0] == 'runner')
        parameters['profile'] = inputs.profile.stringInputs.value[0]

      case 'ga360':
        pass

      case 'sa360':
        if report_id := inputs.report_id.stringInputs.value[0]:
          parameters['report_id'] = report_id
          parameters['type'] = 'sa360_report'
        else:
          parameters['sa360_url'] = inputs.sa360_url.stringInputs.value[0]

    try:
      http = Http()
      resp, content = http.request(
          uri=self.job_manager_uri,
          method='POST',
          headers={'Content-Type': 'application/json; charset=UTF-8'},
          body=json.dumps({'message': parameters}),
      )
      _response = json.loads(content)
      print(_response)

      if response := _response.get('response'):
        success, job = response[0], response[1]
        if success:
          body = self.fetch_job(job_id=job['name'])
          print(body)
          response = self.chat_service.spaces().messages().update(
              name=self.request.message.name,
              updateMask='cardsV2',
              body=body
          ).execute()

        else:
          body = self.message_generator.error(message='Job update failed')
          self.chat_service.spaces().messages().create(
              parent=self.request.space.name,
              body=body
          ).execute()

    except Exception as e:
      trace = error_to_trace(e)
      body = self.message_generator.error(message=f'{trace}')
      self.chat_service.spaces().messages().create(
          parent=self.request.space.name,
          body=body
      ).execute()

    return 'OK'

  def authenticate(self) -> Mapping[str, Any]:
    return {
        'actionResponse': {
            'type': 'REQUEST_CONFIG',
            'url': "https://us-central1-chats-zz9-plural-z-alpha.cloudfunctions.net/report2bq-oauth-start",
        },
    }


def main(unused) -> None:
  del unused
  # f = 'fetch_job.json'
  # f = 'create_job_dv360.json'
  # f = 'create_dv360_message.json'
  # f = 'r.json'
  # f = 'create_cm_message.json'
  # f = 'create_sa360_message.json'
  f = 'list.json'
  # f = 'disable_job.json'
  # f = 'edit_job.json'
  # f = 'x.json'
  with open(f'json/{f}', 'r') as r:
    j = json.load(r)
    print(Report2BQ().process(j))


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
