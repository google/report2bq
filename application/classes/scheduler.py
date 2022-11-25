# Copyright 2020 Google LLC

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
import random
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

from auth.credentials import Credentials
from auth.secret_manager import SecretManager
from google.cloud.scheduler import (CloudSchedulerClient, CreateJobRequest,
                                    DeleteJobRequest, GetJobRequest, Job,
                                    ListJobsRequest, PauseJobRequest,
                                    PubsubTarget, ResumeJobRequest,
                                    UpdateJobRequest)
from googleapiclient.discovery import Resource
from service_framework import service_builder, services

from classes import Fetcher, decorators
from classes.report_type import Type


class Scheduler(Fetcher):
  """Scheduler helper

  Handles the scheduler operations for Report2BQ

  """
  @decorators.lazy_property
  def service(self) -> Resource:
    """Creates the Scheduler service.

    Returns:
        googleapiclient.discovery.Resource: the service
    """
    return service_builder.build_service(service=services.Service.CLOUDSCHEDULER,
                                         key=self.credentials.credentials,
                                         api_key=os.environ['API_KEY'])

  # @decorators.lazy_property
  @property
  def client(self) -> CloudSchedulerClient:
    """Creates the Scheduler service.

    Returns:
        googleapiclient.discovery.Resource: the service
    """
    return CloudSchedulerClient(credentials=self.credentials.credentials)

  @decorators.lazy_property
  def credentials(self) -> Credentials:
    """Generates the Credentials.

    Returns:
        Credentials: the scheduler credentials.
    """
    return Credentials(datastore=SecretManager,
                       email=self.email, project=self.project)

  @decorators.lazy_property
  def location(self) -> str:
    """Lists the available scheduler locations in GCP.

    Returns:
      str: project location id.
    """
    locations_response = self.fetch(
        method=self.service.projects().locations().list,
        **{'name': self.client.common_project_path(self.project)}
    )
    locations = \
        list([location['locationId']
              for location in locations_response['locations']])

    return locations[0]

  @decorators.lazy_property
  def jobs(self) -> List[Job]:
    return self.list_jobs()

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

    match action:
      case 'list':
        return self.jobs

      case 'create' | 'update':
        _attrs = {
            'email': self.email,
            'project': self.project,
        }

        for option in ['force', 'infer_schema', 'append', 'notify_message']:
          if o := kwargs.get(option):
            _attrs[option] = str(o)

        if 'dest_dataset' in kwargs:
          _attrs['dest_dataset'] = kwargs.get('dest_dataset')

        if 'dest_project' in kwargs:
          _attrs['dest_project'] = kwargs.get('dest_project')

        if 'dest_table' in kwargs:
          _attrs['dest_table'] = kwargs.get('dest_table')

        if kwargs.get('minute'):
          _minute = kwargs.get('minute')
        else:
          random.seed(uuid.uuid4())
          _minute = random.randrange(0, 59)

        if kwargs.get('sa360_url'):
          _product = 'sa360'
          _hour = kwargs.get('hour') if kwargs.get('hour') else '3'
          _action = 'fetch'
          _topic = 'report2bq-fetcher'
          _attrs.update({
              'sa360_url': kwargs.get('sa360_url'),
              'type': Type.SA360.value,
          })

        elif kwargs.get('type') == Type.SA360_RPT:
          _product = Type.SA360_RPT.value
          _hour = kwargs.get('hour', '*')
          _action = 'run'
          _topic = 'report2bq-runner'
          _attrs.update({
              'report_id': kwargs.get('report_id'),
              'type': Type.SA360_RPT.value,
          })

        elif kwargs.get('adh_customer'):
          _product = Type.ADH.value
          _hour = kwargs.get('hour') if kwargs.get('hour') else '2'
          _action = 'run'
          _topic = 'report2bq-runner'
          _attrs.update({
              'adh_customer': kwargs.get('adh_customer'),
              'adh_query': kwargs.get('adh_query'),
              'api_key': kwargs.get('api_key'),
              'days': kwargs.get('days'),
              'type': Type.ADH.value,
          })

        elif kwargs.get('type') == Type.GA360_RPT:
          _product = Type.GA360_RPT.value
          _hour = kwargs.get('hour', '*')
          _action = 'run'
          _topic = 'report2bq-runner'
          _attrs.update({
              'report_id': kwargs.get('report_id'),
              'type': Type.GA360_RPT.value,
          })

        else:
          if kwargs.get('runner'):
            _hour = kwargs.get('hour') if kwargs.get('hour') else '1'
            _action = 'run'
            _topic = 'report2bq-runner'
          else:
            _hour = '*'
            _action = 'fetch'
            _topic = 'report2bq-fetcher'

          if kwargs.get('profile'):
            _product = 'cm'
            _attrs.update({
                'profile': kwargs.get('profile'),
                'cm_id': kwargs.get('report_id'),
                'type': Type.CM.value,
            })
          else:
            _product = 'dv360'
            _attrs.update({
                'dv360_id': kwargs.get('report_id'),
                'type': Type.DV360.value,
            })

        name = self.client.job_path(
            self.project, self.location,
            f"{_action}-{_product}-{kwargs.get('report_id')}")

        _target = PubsubTarget(**{
            'topic_name': f"projects/{self.project}/topics/{_topic}",
            'attributes': _attrs,
        })

        job = Job(**{
            'name': name,
            'description': kwargs.get('description'),
            'pubsub_target': _target,
            'schedule': f"{_minute} {_hour} * * *",
            'time_zone': kwargs.get('timezone') or 'UTC',
        })

        f = getattr(self, f'{action}_job')
        return f(job=job)

      case _:
        (success, job) = self.get_job(job_id=kwargs.get('job_id'))

        match action:
          case 'get':
            return success, job

          case 'delete':
            (success, error) = self.delete_job(job_id=job.name)

          case 'enable':
            (success, error) = self.enable_job(job_id=job.name, enable=True)

          case 'disable':
            (success, error) = self.enable_job(job_id=job.name, enable=False)

        return (success, error)

  def list_locations(self) -> List[str]:
    """Lists the available scheduler locations in GCP.

    Returns:
      List[str]: list of location ids.
    """
    locations_response = self.fetch(
        method=self.service.projects().locations().list,
        **{'name': self.client.common_project_path(self.project)}
    )
    locations = \
        list([location['locationId']
              for location in locations_response['locations']])

    return locations

  def list_jobs(self) -> List[Dict[str, Any]]:
    """Lists jobs for a given user.

    Use the scheduler API to fetch all the jobs. Then filter them by the user's
    email. If the user is the administrator, don't filter.

    Returns:
      List[Dict[str, Any]]: [description]
    """
    jobs = []

    ljr = ListJobsRequest(
        parent=self.client.common_location_path(self.project, self.location))
    jobs = self.client.list_jobs(ljr)

    def _filter(job: Dict[str, Any]):
      if _job := job.pubsub_target:
        if _attr := _job.attributes:
          return _attr.get('email') == self.email

      return False

    if self.email and \
            jobs and (self.email != os.environ.get('ADMINISTRATOR_EMAIL')):
      return list(filter(_filter, jobs))
    else:
      return list(jobs)

  def delete_job(self,
                 job_id: str = None) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Deletes a scheduled job.

    Args:
      job_id (str): job id to delete. Defaults to None.

    Returns:
      Tuple[bool, Dict[str, Any]]: success bool, and error if failed.
    """
    try:
      self.client.delete_job(DeleteJobRequest(name=job_id))
      return (True, None)

    except Exception as error:
      logging.error('Error processing job %s: %s',
                    job_id, self.error_to_trace(error))
      return (False, error)

  def enable_job(self, job_id: str = None,
                 enable: bool = True) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Enables or disables a scheduled job.

    enable = True resumes a paused job.
    enable = False pauses an enabled job.

    Args:
      job_id (str, optional): job id to delete. Defaults to None.
      enable (bool, optional): enable action. Defaults to True.

    Returns:
      Tuple[bool, Dict[str, Any]]: (success/fail, error if fail)
    """
    try:
      if enable:
        self.client.resume_job(ResumeJobRequest(name=job_id))
      else:
        self.client.pause_job(PauseJobRequest(name=job_id))

      return (True, None)

    except Exception as error:
      logging.error('Error processing job %s: %s',
                    job_id, self.error_to_trace(error))
      return (False, error)

  def create_job(self, job: Job) -> Tuple[bool, Union[Job, Exception]]:
    """create_job [summary]

    Args:
      job (Job): the job definition. Defaults to None.

    Returns:
      Tuple[bool, Union[Job, Exception]]:
        (success/fail, either the scheduler.Job or error)
    """
    try:
      result = self.client.create_job(
          request=CreateJobRequest(
              parent=self.client.common_location_path(self.project,
                                                      self.location),
              job=job
          ))
      return (True, result)

    except Exception as error:
      logging.error('Error processing job %s: %s',
                    job.name, self.error_to_trace(error))
      return (False, error)

  def update_job(self, job: Job) -> Tuple[bool, Union[Job, Exception]]:
    """create_job [summary]

    Args:
      job (Job): the job definition. Defaults to None.

    Returns:
      Tuple[bool, Union[Job, Exception]]:
        (success/fail, either the scheduler.Job or error)
    """
    try:
      result = self.client.update_job(request=UpdateJobRequest(job=job))
      return (True, result)

    except Exception as error:
      logging.error('Error processing job %s: %s',
                    job.name, self.error_to_trace(error))
      return (False, error)

  def get_job(self, job_id: str) -> Tuple[bool, Union[Job, Exception]]:
    """Gets a job definition from the scheduler.

    Args:
      job_id (str, optional): [description]. Defaults to None.

    Returns:
      Tuple[bool, Union[Job, Exception]]: success indicator, either Job
        definition or Exception.
    """
    job_name = self.client.job_path(
        location=self.location, project=self.project, job=job_id)
    try:
      job = self.client.get_job(request=GetJobRequest(name=job_name))
      return (True, job)

    except Exception as error:
      logging.error('Error processing job %s: %s',
                    job_name, self.error_to_trace(error))
      return (False, error)

  def _find(self, lst: List[Job], name: str) -> Job:
    raw_job = '/' + name.split('-')[-1]
    for job in lst:
      if job['name'].endswith(name) or job['name'].endswith(raw_job):
        return job

    raise Exception(f'Job {name} not found in job list.')
