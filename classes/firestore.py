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

from typing import Dict, List, Any

# Class Imports
from classes.credentials import Credentials
from classes.report_type import Type

from google.cloud import firestore
from google.cloud import bigquery
from google.cloud.firestore import DocumentReference

from typing import Any, Dict, Tuple


class Firestore(object):
  """Firestore Helper

  This class handles all Firestore interactions.

  """

  def __init__(self, email: str=None, project: str=None, in_cloud: bool=True):
    """constructor

    Takes email address, project and in_cloud to access the credentials needed. If it's running
    in the cloud (in_cloud=True), is assumes it can simply use the project default service account
    creds to access Firestore. This is different to the DV360/CM/SA360/ADH fetchers as they have
    to use a person's real credentials.
    
    Keyword Arguments:
        email {str} -- email address of the credentials (default: {None})
        project {str} -- project identifier (default: {None})
        in_cloud {bool} -- is it running in the cloud (default: {True})
    """
    if in_cloud:
      self.client = firestore.Client()

    else:
      self.client = firestore.Client(credentials=Credentials(in_cloud=in_cloud, email=email, project=project).get_credentials())


  def store_oauth_state(self, state: str, email: str, project: str) -> None:
    key = 'oauth/{state}'.format(state=state)
    data = {
      'email': email,
      'project': project
    }
    self.client.document(key).set(data)


  def get_oauth_state(self, state: str) -> Tuple[str, str]:
    key = 'oauth/{state}'.format(state=state)
    data = self.client.document(key)
    email = project = None
    if data:
      values = data.get().to_dict()
      email = values.get('email')
      project = values.get('project')

    return email, project


  def delete_oauth_state(self, state: str) -> None:
    key = 'oauth/{state}'.format(state=state)
    data = self.client.document(key)
    data.delete()
    

  def get_report_config(self, type: Type, id: str) -> Dict[str, Any]:
    """Load a config

    Load a report's config
    
    Arguments:
        type {Type} -- report type
        id {str} -- report id
    
    Returns:
        Dict[str, Any] -- stored configuration dictionary
    """
    config = None

    report_name = '{type}/{report}'.format(type=type.value, report=id)
    report = self.client.document(report_name)
    if report:
      config = report.get().to_dict()

    return config


  def store_report_config(self, type: Type, id: str, report_data: Dict[str, Any]):
    """Store a config

    Store a report's config in Firestore. They're all stored by Type (DCM/DBM/SA360/ADH)
    and each one within the type is keyed by the appropriate report id.
    
    Arguments:
        type {Type} -- product
        id {str} -- report id
        report_data {Dict[str, Any]} -- report configuration
    """
    report_name = '{type}/{report}'.format(type=type.value, report=id)
    report = self.client.document(report_name)
    if report:
      self.client.document(report_name).delete()

    self.client.document(report_name).set(report_data)


  def get_all_reports(self, type: Type) -> List[Dict[str, Any]]:
    """List all reports

    List all defined reports for a specific product.
    
    Arguments:
        type {Type} -- product type
    
    Returns:
        List[Dict[str, Any]] -- list of all reports for a given project
    """
    reports = []
    collection = self.client.collection(type.value).list_documents()
    for document in collection:
      reports.append(document.get().to_dict())

    return reports


  def store_import_job_details(self, report_id: int, job: bigquery.LoadJob):
    """Save a BQ Import job in Firestore
    
    Arguments:
        report_id {int} -- [description]
        job {bigquery.LoadJob} -- [description]
    """
    document = 'jobs/{report_id}'.format(report_id=report_id)
    self.client.document(document).set(job.to_api_repr())


  def mark_import_job_complete(self, report_id: int, job: bigquery.LoadJob):
    """Mark BQ Import job in Firestore done
    
    Moves an import job from 'jobs/' to 'jobs-completed'.
    
    Arguments:
        report_id {int} -- [description]
        job {bigquery.LoadJob} -- [description]
    """
    document = 'jobs/{report_id}'.format(report_id=report_id)
    self.client.document(document).delete()
    
    document = 'jobs-completed/{report_id}'.format(report_id=report_id)
    self.client.document(document).set(job.to_api_repr())
    

  def get_all_jobs(self) -> List[DocumentReference]:
    """List all running jobs
    
    Returns:
        jobs {List[DocumentReference]} -- List of all available jobs
    """
    jobs = []
    collection = self.client.collection('jobs').list_documents()
    for document in collection:
      jobs.append(document)

    return jobs


  def get_all_running(self) -> List[DocumentReference]:
    """List running reports

    Lists all running reports
    
    Returns:
        runners {List[DocumentReference]} -- list of all running reports
    """
    runners = []
    collection = self.client.collection('running').list_documents()
    for document in collection:
      runners.append(document)

    return runners


  def store_report_runner(self, runner: Dict[str, Any]):
    """Store running report
    
    Arguments:
        runner {Dict[str, Any]} -- store a running report definition
    """
    document = 'running/{report_id}'.format(report_id=runner['report_id'])
    self.client.document(document).set(runner)


  def remove_report_runner(self, runner: str):
    """Remove running report

    Delete a running report from the list of active reports
    
    Arguments:
        runner {Dict[str, Any]} -- [description]
    """
    document = f'running/{runner}'
    self.client.document(document).delete()
