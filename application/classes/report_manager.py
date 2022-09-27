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

import dataclasses
import enum
import json
import logging
import os
import random
import gcsfs
import uuid

from typing import Any, Dict, Iterable, List, Optional
from classes import error_to_trace

from classes.cloud_storage import Cloud_Storage
from classes.decorators import lazy_property
from classes.firestore import Firestore
from classes.report_type import Type
from classes.scheduler import Scheduler
from classes.query.report_manager import ManagerInput


class ManagerType(enum.Enum):
  BIG_QUERY = enum.auto()
  FILE_LOCAL = enum.auto()
  FILE_GCS = enum.auto()


@dataclasses.dataclass
class ManagerConfiguration(object):
  type: ManagerType
  project: str
  email: str
  table: str
  dataset: str = 'report2bq_admin'
  file: Optional[str] = None

  @property
  def gcs_stored(self) -> bool:
    return self.type == ManagerType.FILE_GCS


class ReportManager(object):
  """Abstract parent ReportManager for SA360 and GA360 reports.
  """
  report_type: Type = None
  bucket: str = None
  actions: set = None

  @lazy_property
  def scheduler(self) -> Scheduler:
    """Lazy init for the scheduler

    Returns None if no API Key is provided, which just means that no scheduler
    functionality will be available.

    Returns:
        Scheduler: The scheduler
    """
    return Scheduler() if 'API_KEY' in os.environ else None

  @lazy_property
  def firestore(self) -> Firestore:
    """The Firestore client wrapper

    Returns:
        Firestore: the wrapper
    """
    return Firestore()

  @firestore.setter
  def firestore(self, f: Any) -> None:
    self._lazy_firestore = f

  def manage(self, **kwargs: Dict[str, Any]) -> Any:
    """The control function

    For the parent class, this should be empty.

    Returns:
        Any: the return from the specific function.
    """
    pass

  def add(self, report: str, config: ManagerConfiguration, **unused) -> None:
    """Add a report

    Add a report to the firestore for this 'Type' of product.

    Args:
        report (str): report name
        config (ManagerConfiguration): the configuration details
    """
    if cfg := self._read_json(config=config):
      self.firestore.update_document(self.report_type,
                                     '_reports',
                                     {report: cfg})

  def delete(self, report: str, config: ManagerConfiguration, **unused) -> None:
    """Delete reports from Firestore

    Remove the report from Firestore. If a scheduler is available, it will also
    disable (not delete) all scheduled jobs relying on this report.

    This can ONLY be done through the file-based mechanism.

    Args:
        report (str): report name
        config (ManagerConfiguration): the configuration details
    """
    if config.type == ManagerType.BIG_QUERY:
      raise TypeError('Delete action not valid for BQ configurations.')

    self.firestore.delete_document(self.report_type, '_reports', report)

    if email := self._read_email(file=config.file,
                                 gcs_stored=config.gcs_stored):
      if self.scheduler:
        args = {
            'action': 'list',
            'email': email,
            'project': config.project,
            'html': False,
        }

        # Disable all runners for the now deleted report
        runners = list(
            runner['name'].split('/')[-1]
            for runner in self.scheduler.process(**args)
            if report in runner['name'])
        for runner in runners:
          args = {
              'action': 'disable',
              'email': email,
              'project': config.project,
              'job_id': runner,
          }
          self.scheduler.process(**args)
    else:
      logging.error('No email found, cannot access scheduler.')
      return

  def list(self, report: str, config: ManagerConfiguration,
           **unused) -> List[str]:
    """List all reports and runners.

    List the reports and runners stored in Firestore

    Args:
        report (str): report name
        config (ManagerConfiguration): the configuration details

    Returns:
        List[str]: [description]
    """
    objects = self.firestore.list_documents(self.report_type)
    reports = self.firestore.list_documents(self.report_type, '_reports')
    results = []
    for report in reports:
      results.append(f'{report}')
      for object in objects:
        if object.startswith(report):
          results.append(f'  {object}')
      self._output_results(results=results,
                           project=config.project, email=config.email,
                           file='report_list', gcs_stored=config.gcs_stored)

    return results

  def show(self, report: str, config: ManagerConfiguration,
           **unused) -> Dict[str, Any]:
    """Display a report definition content.

    Writes the json definition of the requested report to a file, either
    locally or in GCS.

    Args:
        report (str): report name
        config (ManagerConfiguration): the configuration details

    Raises:
        NotImplementedError: default behaviour if unimplemented.

    Returns:
        Dict[str, Any]: the report definition as json.
    """
    if config.type == ManagerType.BIG_QUERY:
      raise TypeError('Show action not valid for BQ configurations.')

    definition = \
        self.firestore.get_document(self.report_type, '_reports').get(report)
    results = [l for l in json.dumps(definition, indent=2).splitlines()]

    self._output_results(results=results, project=config.project, email=None,
                         file=report, gcs_stored=config.gcs_stored)

    return definition

  def install(self, config: ManagerConfiguration, **unused) -> None:
    """Install a report runner or multiple report runners.

    To be implemented by the child.

    Args:
        config (ManagerConfiguration): the configuration details

    Raises:
        NotImplementedError: default behaviour if unimplemented.
    """
    raise NotImplementedError('Not implemented')

  def validate(self, config: ManagerConfiguration, **unused) -> None:
    """Validate report runners.

    To be implemented by the child.

    Args:
        config (ManagerConfiguration): the configuration details

    Raises:
        NotImplementedError: default behaviour if unimplemented.
    """
    raise NotImplementedError('Not implemented')

  def _output_results(
          self, results: List[str], project: str, email: str, file: str = None,
          gcs_stored: bool = False) -> None:
    """Write the process results to a file.

    Args:
        results (List[str]): the results.
        project (str): project id
        email (str): OAuth email
        file (str, optional): file to process. Defaults to None.
        gcs_stored (bool, optional): write to GCS? Defaults to False.
    """
    def _send():
      for result in results:
        print(result, file=outfile)

    output_name = f'{file}.results'
    if gcs_stored:
      fs = gcsfs.GCSFileSystem(project=project)
      with fs.open(f'{self.bucket}/{output_name}', 'w') as outfile:
        _send()

    else:
      with open(output_name, 'w') as outfile:
        _send()

  def _get_action(self, action_name: str) -> Any:
    """Determines the function to run.

    Checks the 'actions' set for a match between the requested action and
    one which exists. It then returns the function to be executed.

    Args:
        action_name (str): the action name to execute

    Returns:
        Any: the action function
    """
    if action := getattr(self, action_name) \
            if action_name in self.actions else None:
      return action

    else:
      raise NotImplementedError(f'Action "{action_name}" is not implemented.')

  def _read_email(self, file: str, gcs_stored: bool) -> str:
    """Read an email address from a file.

    Args:
        file (str): the file to process.
        gcs_stored (bool): is the file local or GCS?

    Returns:
        str: the email address
    """
    if gcs_stored:
      email = str(Cloud_Storage().fetch_file(bucket=self.bucket,
                                             file=file),
                  encoding='utf-8').strip()

    else:
      with open(file, 'r') as _command_file:
        email = _command_file.readline().strip()

    return email

  def _read_json(self, config: ManagerConfiguration) -> List[Dict[str, Any]]:
    """Read the contens of a file as a json object.

    Args:
        config (ManagerConfiguration): the manager configuration

    Returns:
        List[Dict[str, Any]]: the file contents as json
    """
    objects = []

    if config.type == ManagerType.BIG_QUERY:
      query = ManagerInput(config)
      job = query.execute()
      objects = [dict(row) for row in job]

    else:
      if config.file:
        if config.gcs_stored:
          content = \
              Cloud_Storage(project=config.project,
                            email=config.email).fetch_file(bucket=self.bucket,
                                                           file=config.file)
          objects = json.loads(content)
        else:
          with open(config.file) as rpt:
            objects = json.loads(''.join(rpt.readlines()))

      else:
        objects = self.firestore.list_documents(self.report_type)

    return objects

  def _schedule_job(self, project: str, runner: Dict[str, Any], id: str) -> str:
    random.seed(uuid.uuid4())
    job_id = f"run-{self.report_type}-{id}"

    args = {
        'action': 'get',
        'email': runner['email'],
        'project': f'{project}',
        'job_id': job_id,
    }

    try:
      present, _ = self.scheduler.process(**args)

    except Exception as e:
      err = error_to_trace(e)
      logging.error('%s - Check if already defined failed: %s',
                    job_id, err)
      return f'{job_id} - Check if already defined failed: {err}'

    if present:
      args = {
          'action': 'delete',
          'email': runner['email'],
          'project': f'{project}',
          'job_id': job_id,
      }
      try:
        self.scheduler.process(**args)

      except Exception as e:
        logging.error('%s - Already present but delete failed: %s', job_id, e)
        return f'{job_id} - Already present but delete failed: {e}'

    args = {
        'action': 'create',
        'email': runner['email'],
        'project': f'{project}',
        'force': False,
        'infer_schema': runner.get('infer_schema', False),
        'append': runner.get('append', False),
        'report_id': id,
        'description': runner.get('description'),
        'minute': runner.get('minute', random.randrange(0, 59)),
        'hour': runner.get('hour', '*'),
        'type': self.report_type,
    }
    if dest_project := runner.get('dest_project'):
      args['dest_project'] = dest_project
    if dest_dataset := runner.get('dest_dataset'):
      args['dest_dataset'] = runner.get('dest_dataset')
    if dest_table := runner.get('dest_table'):
      args['dest_table'] = dest_table

    try:
      self.scheduler.process(**args)
      return f'{job_id} - Valid and installed.'

    except Exception as e:
      logging.error('%s - Failed to create: %s', job_id, e)
      return f'{job_id} - Failed to create: {e}'

  def _chunk(self, thing: Iterable[Any], size: int):
    """Yield successive n-sized chunks from thing."""
    for i in range(0, len(thing), size):
      yield thing[i:i + size]
