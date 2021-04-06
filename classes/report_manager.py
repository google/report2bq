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

import io
import json
import logging
import os

from typing import Any, Dict, List

from classes.cloud_storage import Cloud_Storage
from classes.decorators import lazy_property
from classes.firestore import Firestore
from classes.report_type import Type
from classes.scheduler import Scheduler


class ReportManager(object):
  """Abstract parent ReportManager for SA360 and GA360 reports.
  """
  report_type: Type=None
  bucket: str=None

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

  def manage(self, **kwargs: Dict[str, Any]) -> Any:
    """The control function

    For the parent class, this should be empty.

    Returns:
        Any: the return from the specific function.
    """
    pass

  def add(self, report: str, True: str, file: str,
          gcs_stored: bool=True, project: str=None, email: str=None,
          **unused) -> None:
    """Add a report

    Add a report to the firestore for this 'Type' of product.

    Args:
        report (str): report name
        file (str): file containing the report definition
        gcs_stored (bool, optional): running in GCS? Defaults to True.
        project (str, optional): project id. Defaults to None.
        email (str, optional): OAuth email. Defaults to None.
    """
    if cfg := self._read_json(project=project,
                              email=email, file=file, gcs_stored=gcs_stored):
      self.firestore.update_document(self.report_type,
                                     '_reports',
                                     { report: cfg })


  def delete(self, report: str, file: str,
             gcs_stored: bool=True, project: str=None, email: str=None,
             **unused) -> None:
    """Delete reports from Firestore

    Remove the report from Firestore. If a scheduler is available, it will also
    disable (not delete) all scheduled jobs relying on this report.

    Args:
        report (str): the report name
        file (str): the file to process
        gcs_stored (bool, optional): Am I in GCS?. Defaults to True.
        project (str, optional): the project id. Defaults to None.
        email (str, optional): email address for OAuth. Defaults to None.
    """
    self.firestore.delete_document(Type.SA360_RPT, '_reports', report)

    if email := self._read_email(file=file, gcs_stored=gcs_stored):
      if self.scheduler:
        args = {
          'action': 'list',
          'email': email,
          'project': project,
          'html': False,
        }

        # Disable all runners for the now deleted report
        runners = list(
          runner['name'].split('/')[-1] \
            for runner in self.scheduler.process(args) \
              if report in runner['name'])
        for runner in runners:
          args = {
            'action': 'disable',
            'email': email,
            'project': project,
            'job_id': runner,
          }
          self.scheduler.process(args)
    else:
      logging.error('No email found, cannot access scheduler.')
      return

  def list(self, report: str, file: str,
           gcs_stored: bool=True, project: str=None, email: str=None,
           **unused) -> List[str]:
    """List all reports and runners.

    List the reports and runners stored in Firestore

    Args:
        report (str): report name
        file (str): file to process
        gcs_stored (bool, optional): am  in GCS? Defaults to True.
        project (str, optional): [description]. Defaults to None.
        email (str, optional): [description]. Defaults to None.

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
      self._output_results(
        results=results, project=project, email=email, file='report_list',
        gcs_stored=gcs_stored)

    return results

  def show(self, report: str, file: str,
           gcs_stored: bool=True, project: str=None, email: str=None,
           **unused) -> Dict[str, Any]:
    """Display a report definition content.

    To be implemented by the child.

    Args:
        report (str): report name
        file (str): file to process
        gcs_stored (bool, optional): in GCS? Defaults to True.
        project (str, optional): project id. Defaults to None.
        email (str, optional): OAuth email. Defaults to None.

    Raises:
        NotImplementedError: default behaviour if unimplemented.

    Returns:
        Dict[str, Any]: the report definition as json.
    """
    raise NotImplementedError('Not implemented')

  def install(self, project: str, email: str, file: str,
              gcs_stored: bool=True, **unused) -> None:
    """Install a report runner or multiple report runners.

    To be implemented by the child.

    Args:
        file (str): file to process
        gcs_stored (bool, optional): in GCS? Defaults to True.
        project (str, optional): project id. Defaults to None.
        email (str, optional): OAuth email. Defaults to None.

    Raises:
        NotImplementedError: default behaviour if unimplemented.
    """
    raise NotImplementedError('Not implemented')

  def validate(self, file: str, project: str, email: str,
               gcs_stored: bool=True,**unused) -> None:
    """Validate report runners.

    To be implemented by the child.

    Args:
        file (str): file to process
        project (str, optional): project id. Defaults to None.
        email (str, optional): OAuth email. Defaults to None.
        gcs_stored (bool, optional): in GCS? Defaults to True.

    Raises:
        NotImplementedError: default behaviour if unimplemented.
    """
    raise NotImplementedError('Not implemented')

  def _output_results(
    self, results: List[str], project: str, email: str, file: str=None,
    gcs_stored: bool=False) -> None:
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
      outfile = io.StringIO()
      _send()

      Cloud_Storage(project=project,
                    email=email).write_file(bucket=self.bucket,
                                            file=output_name,
                                            data=outfile.getvalue())

    else:
      with open(output_name, 'w') as outfile:
        _send()

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

  def _read_json(self,
                 project: str, email: str, file: str,
                 gcs_stored: bool) -> Dict[str, Any]:
    """Read the contens of a file as a json object.

    Args:
        project (str): project id
        email (str): OAuth email
        file (str): file to process
        gcs_stored (bool): is the file GCS or local?

    Returns:
        Dict[str, Any]: the file contents as json
    """
    if gcs_stored:
      content = \
        Cloud_Storage(project=project,
                      email=email).fetch_file(bucket=self.bucket, file=file)
      cfg = json.loads(content)

    else:
      with open(file) as definition:
        cfg = json.loads(''.join(definition.readlines()))

    return cfg
