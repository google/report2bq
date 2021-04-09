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

import json
import os
import random
import uuid

# Class Imports
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud import scheduler as scheduler
from google.cloud.scheduler_v1.types.job import Job
from googleapiclient.errors import HttpError

from classes import decorators
from classes import Fetcher
from classes.credentials import Credentials
from classes import discovery
from classes.report_type import Type
from classes.services import Service


class Scheduler(Fetcher):
  """Scheduler helper

  Handles the scheduler operations for Report2BQ

  """
  @decorators.lazy_property
  def service(self):
    """Creates the Scheduler service.

    Returns:
        googleapiclient.discovert.Resource: the service
    """
    return discovery.get_service(service=Service.SCHEDULER,
                                 credentials=self.credentials,
                                 api_key=os.environ['API_KEY'])

  @decorators.lazy_property
  def credentials(self) -> Credentials:
    """Generates the Credentials.

    Returns:
        Credentials: the scheduler scredentials.
    """
    return Credentials(email=self.email, project=self.project)

  @decorators.lazy_property
  def location(self) -> str:
    """Lists the available scheduler locations in GCP.

    Returns:
      str: project location id.
    """
    locations_response = self.fetch(
      method=self.service.projects().locations().list,
      **{'name': self.project_path}
    )
    locations = \
      list([ location['locationId'] \
        for location in locations_response['locations'] ])

    return locations[-1]

  @decorators.lazy_property
  def location_path(self) -> str:
    """Returns a fully-qualified location string."""
    return f'locations/{self.location}'

  @decorators.lazy_property
  def project_path(self) -> str:
    """Returns a fully-qualified project string."""
    return f'projects/{self.project}'

  def process(self, action: str, project: str, email: str, **kwargs) -> Any:
    """Processes the main scheduler requests.

    This is a single point of entry, taking a Dict of parameters.

    Args:
      action (str): the action to perform
      project (str): the project
      emails (str): the user's email
      kwargs (Dict[str, Any]): the process specific arguments

    Returns:
      Any: the result of the processing
    """
    self.project = project
    self.email = email

    locations = self.list_locations()
    _location = locations[-1]

    if action == 'list':
      jobs = self.list_jobs()
      return jobs

    elif action == 'get':
      (success, job) = self.get_job(job_id=kwargs.get('job_id'))
      return success, job

    elif action == 'delete':
      (success, error) = self.delete_job(job_id=kwargs.get('job_id'))

      if success:
        return 'OK'
      else:
        return f'ERROR!\n{error["error"]["message"]}'

    elif action == 'enable':
      (success, error) = \
        self.enable_job(job_id=kwargs.get('job_id'), enable=True)

      if success:
        return 'OK'
      else:
        return f'ERROR!\n{error["error"]["message"]}'

    elif action == 'disable':
      (success, error) = \
        self.enable_job(job_id=kwargs.get('job_id'), enable=False)

      if success:
        return 'OK'
      else:
        return f'ERROR!\n{error["error"]["message"]}'

    elif action == 'create':
      _attrs = {
        'email': self.email,
        'project': self.project,
        'force': str(kwargs.get('force')),
        'infer_schema': str(kwargs.get('infer_schema')),
        'append': str(kwargs.get('append')),
      }

      if 'dest_dataset' in kwargs:
        _attrs['dest_dataset'] = kwargs.get('dest_dataset')

      if 'dest_project' in kwargs:
        _attrs['dest_project'] = kwargs.get('dest_project')

      if 'dest_table' in kwargs:
        _attrs['dest_table'] = kwargs.get('dest_table')

      if kwargs.get('minute'):
        minute = kwargs.get('minute')
      else:
        random.seed(uuid.uuid4())
        minute = random.randrange(0, 59)

      if kwargs.get('sa360_url'):
        product = _type = 'sa360'
        hour = kwargs.get('hour') if kwargs.get('hour') else '3'
        action = 'fetch'
        topic = 'report2bq-trigger'
        _attrs.update({
          'sa360_url': kwargs.get('sa360_url'),
          'type': Type.SA360.value,
        })

      elif kwargs.get('type') == Type.SA360_RPT:
        product = _type = Type.SA360_RPT.value
        hour = kwargs.get('hour', '*')
        action = 'run'
        topic = 'report-runner'
        _attrs.update({
          'report_id': kwargs.get('report_id'),
          'type': Type.SA360_RPT.value,
        })

      elif kwargs.get('adh_customer'):
        product = _type = Type.ADH.value
        hour = kwargs.get('hour') if kwargs.get('hour') else '2'
        action = 'run'
        topic = 'report2bq-trigger'
        _attrs.update({
          'adh_customer': kwargs.get('adh_customer'),
          'adh_query': kwargs.get('adh_query'),
          'api_key': kwargs.get('api_key'),
          'days': kwargs.get('days'),
          'type': Type.ADH.value,
        })

      elif kwargs.get('type') == Type.GA360_RPT:
        product = _type = Type.GA360_RPT.value
        hour = kwargs.get('hour', '*')
        action = 'run'
        topic = 'report-runner'
        _attrs.update({
          'report_id': kwargs.get('report_id'),
          'type': Type.GA360_RPT.value,
        })

      else:
        if kwargs.get('runner'):
          hour = kwargs.get('hour') if kwargs.get('hour') else '1'
          action = 'run'
          topic = 'report-runner'
        else:
          hour = '*'
          action = 'fetch'
          topic = 'report2bq-trigger'

        if kwargs.get('profile'):
          product = 'cm'
          _type = 'cm'
          _attrs.update({
            'profile': kwargs.get('profile'),
            'cm_id': kwargs.get('report_id'),
            'type': Type.CM.value,
          })
        else:
          product = 'dv360'
          _type = 'dv360'
          _attrs.update({
            'dv360_id': kwargs.get('report_id'),
            'type': Type.DV360.value,
          })

      name = f"{action}-{product}-{kwargs.get('report_id')}"
      schedule = f"{minute} {hour} * * *"

      job = {
        'description': kwargs.get('description'),
        'timeZone': kwargs.get('timezone') or 'UTC',
        'api_key': kwargs.get('api_key'),
        'name': name,
        'schedule': schedule,
        'topic': topic,
        'attributes': _attrs,
      }

      self.create_job(job=job)

  def list_locations(self) -> List[str]:
    """Lists the available scheduler locations in GCP.

    Returns:
      List[str]: list of location ids.
    """
    locations_response = self.fetch(
      method=self.service.projects().locations().list,
      **{'name': self.project_path}
    )
    locations = \
      list([ location['locationId'] \
        for location in locations_response['locations'] ])

    return locations

  def list_jobs(self) -> List[Dict[str, Any]]:
    """Lists jobs for a given user.

    Use the scheduler API to fetch all the jobs. Then filter them by the user's
    email. If the user is the administrator, don't filter.

    Returns:
      List[Dict[str, Any]]: [description]
    """
    token = None
    func = self.service.projects().locations().jobs().list
    jobs = []

    while True:
      _kwargs = {
        'parent': f'{self.project_path}/{self.location_path}',
        'pageToken': token
      }

      _jobs = self.fetch(func, **_kwargs)
      jobs.extend(_jobs['jobs'] if 'jobs' in _jobs else [])

      if 'nextPageToken' not in _jobs:
        break

      token = _jobs['nextPageToken']

    def _filter(job: Dict[str, Any]):
      if _job := job.get('pubsubTarget'):
        if _attr := _job.get('attributes'):
          return _attr.get('email') == self.email

      return False

    if self.email and \
      jobs and (self.email != os.environ.get('ADMINISTRATOR_EMAIL')):
      job_list = filter(_filter, jobs)

    return list(job_list)

  def delete_job(self,
                 job_id: str=None) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Deletes a scheduled job.

    Args:
      job_id (str, optional): job id to delete. Defaults to None.

    Returns:
      Tuple[bool, Dict[str, Any]]: success bool, and error if failed.
    """
    func = self.service.projects().locations().jobs().delete

    try:
      func(name=self.job_path(job=job_id)).execute()
      return (True, None)

    except HttpError as error:
      e = json.loads(error.content)
      return (False, e)

  def enable_job(self, job_id: str=None,
                 enable: bool=True) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Enables or disables a scheduled job.

    enable = True resumes a paused job.
    enable = False pauses an enabled job.

    Args:
      job_id (str, optional): job id to delete. Defaults to None.
      enable (bool, optional): enable action. Defaults to True.

    Returns:
      Tuple[bool, Dict[str, Any]]: (success/fail, error if fail)
    """
    if enable:
      func = self.service.projects().locations().jobs().resume
    else:
      func = self.service.projects().locations().jobs().pause

    try:
      func(name=self.job_path(job=job_id)).execute()
      return (True, None)

    except HttpError as error:
      e = json.loads(error.content)
      return (False, e)

  def create_job(self,
                 job: Dict[str, Any]) -> Tuple[bool, Union[Job, Exception]]:
    """create_job [summary]

    Args:
      job (Dict[str, Any], optional): the job definition. Defaults to None.

    Returns:
      Tuple[bool, Union[Job, Exception]]:
        (success/fail, either the scheduler.Job or error)
    """
    func = self.service.projects().locations().jobs().create

    _target = {
      'topicName': f"projects/{self.project}/topics/{job.get('topic', '')}",
      'attributes': job.get('attributes', ''),
    }
    body: dict = {
      "name": self.job_path(job=job.get('name', '')),
      "description": job.get('description', ''),
      "schedule": job.get('schedule', ''),
      "timeZone": job.get('timezone', ''),
      'pubsubTarget': _target
    }

    _args = {
      'parent': f'{self.project_path}/{self.location_path}',
      'body': body
    }

    try:
      request = func(**_args)
      scheduled_job: Job = request.execute()
      return (True, scheduled_job)

    except Exception as error:
      return (False, error)

  def get_job(self, job_id: str) -> Tuple[bool, Union[Job, Exception]]:
    """Gets a job definition from the scheduler.

    Args:
      job_id (str, optional): [description]. Defaults to None.

    Returns:
      Tuple[bool, Union[Job, Exception]]: success indicator, either Job
        definition or Exception.
    """
    func = self.service.projects().locations().jobs().get

    try:
      job = func(
        name=self.job_path(job=job_id)).execute()
      return (True, job)

    except HttpError as error:
      e = json.loads(error.content)
      return (False, e)

  def job_path(self, job: str) -> str:
    """Returns a fully-qualified job string.

    Args:
      job (str): [description]

    Returns:
      str: [description]
    """
    return f'{self.project_path}/{self.location_path}/jobs/{job}'
