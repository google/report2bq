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
import logging

from contextlib import suppress
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
      self.client = \
        firestore.Client(credentials=Credentials(email=email,
                         project=project).get_credentials())


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
      _doc = document.get().to_dict()
      _doc['_id'] = document.id
      reports.append(_doc)

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
    return self.get_all_documents(Type._JOBS)


  def get_all_running(self) -> List[DocumentReference]:
    """List running reports

    Lists all running reports

    Returns:
        runners {List[DocumentReference]} -- list of all running reports
    """
    return self.get_all_documents(Type._RUNNING)


  def get_all_documents(self, type: Type) -> List[DocumentReference]:
    """List documents

    Lists all documents of a given Type

    Returns:
        runners {List[DocumentReference]} -- list of all documents
    """
    runners = []
    collection = self.client.collection(type.value).list_documents()
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


  def get_document(self, type: Type, id: str) -> Dict[str, Any]:
    """Load a document (could be anything, 'type' identifies the root.)

    Load a document

    Arguments:
        type {Type} -- document type (document root in firestore)
        id {str} -- document id

    Returns:
        Dict[str, Any] -- stored configuration dictionary, or None
                          if not present
    """
    config = None

    report_name = f'{type.value}/{id}'
    report = self.client.document(report_name)
    if report:
      config = report.get().to_dict()

    return config


  def store_document(self, type: Type, id: str, document: Dict[str, Any]):
    """Store a config

    Store a report's config in Firestore. They're all stored by Type (DCM/DBM/SA360/ADH)
    and each one within the type is keyed by the appropriate report id.

    Arguments:
        type {Type} -- product
        id {str} -- report id
        report_data {Dict[str, Any]} -- report configuration
    """
    report_name = f'{type.value}/{id}'
    report = self.client.document(report_name)
    if report:
      self.client.document(report_name).delete()

    self.client.document(report_name).set(document)


  def update_document(self, type: Type, id: str, new_data: Dict[str, Any]):
    _existing = self.get_document(type=type, id=id) or {}
    _existing.update(new_data)

    self.store_document(type=type, id=id, document=_existing)


  def delete_document(self, type: Type, id: str, key: str):
    _existing = self.get_document(type=type, id=id)
    if not _existing:
      logging.info(f'Document {type}/{id} does not exist.')
      return

    with suppress(KeyError):
      _existing.pop(key)

    self.store_document(type=type, id=id, document=_existing)


  def list_documents(self, report_type: Type, key: str=None) -> List[str]:
    documents = []
    collection = self.client.collection(f'{report_type.value}').list_documents()
    for document in collection:
      if key:
        if document.id == key:
          for _document in document.get().to_dict():
            documents.append(_document)
      else:
        documents.append(document.id)

    return documents
