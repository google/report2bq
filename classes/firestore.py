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

from typing import Any, Dict, List, Optional

from classes import decorators
from classes.credentials import Credentials
from classes.report_type import Type

from google.cloud import firestore
from google.cloud import bigquery
from google.cloud.firestore import DocumentReference

from typing import Any, Dict


class Firestore(object):
  """Firestore Helper

  This class handles all Firestore interactions.

  """
  def __init__(self, email: str=None, project: str=None, in_cloud: bool=True):
    """constructor

    Takes email address, project and in_cloud to access the credentials needed.
    If it's running in the cloud (in_cloud=True), is assumes it can simply use
    the project default service account creds to access Firestore. This is
    different to the DV360/CM/SA360/ADH fetchers as they have to use a person's
    real credentials.

    Keyword Arguments:
        email {str} -- email address of the credentials (default: {None})
        project {str} -- project identifier (default: {None})
        in_cloud {bool} -- is it running in the cloud (default: {True})
    """
    self._in_cloud = in_cloud
    self._project = project
    self._email = email

  @decorators.lazy_property
  def client(self):
    return firestore.Client() if self._in_cloud else \
        firestore.Client(credentials=Credentials(email=self._email,
                         project=self._project).get_credentials())

  def get_report_config(self, type: Type, id: str) -> Dict[str, Any]:
    """Loads a config

    Load a report's config

    Arguments:
        type {Type} -- report type
        id {str} -- report id

    Returns:
        Dict[str, Any] -- stored configuration dictionary
    """
    return self.get_document(type, id)

  def store_report_config(self, type: Type, id: str,
                          report_data: Dict[str, Any]) -> None:
    """Stores a config

    Store a report's config in Firestore. They're all stored by Type
    (DCM/DBM/SA360/ADH) and each one within the type is keyed by the
    appropriate report id.

    Arguments:
        type {Type} -- product
        id {str} -- report id
        report_data {Dict[str, Any]} -- report configuration
    """
    self.store_document(type=type, id=id, document=report_data)

  def get_all_reports(self, type: Type) -> List[Dict[str, Any]]:
    """Lists all reports

    List all defined reports for a specific product.

    Arguments:
        type {Type} -- product type

    Returns:
        List[Dict[str, Any]] -- list of all reports for a given project
    """
    reports = []
    collection = self.client.collection(type.value).list_documents()
    for document in collection:
      doc = document.get().to_dict()
      doc['_id'] = document.id
      reports.append(doc)

    return reports

  def store_import_job_details(self, report_id: int,
                               job: bigquery.LoadJob) -> None:
    """Saves a BQ Import job in Firestore

    Arguments:
        report_id {int} -- [description]
        job {bigquery.LoadJob} -- [description]
    """
    self.store_document(Type._JOBS, report_id, job.to_api_repr())

  def mark_import_job_complete(self, report_id: int,
                               job: bigquery.LoadJob) -> None:
    """Marks a BQ Import job in Firestore done

    Moves an import job from 'jobs/' to 'jobs-completed'.

    Arguments:
        report_id {int} -- [description]
        job {bigquery.LoadJob} -- [description]
    """
    self.delete_document(Type._JOBS, report_id)
    self.store_document(Type._COMPLETED, report_id, job.to_api_repr())

  def get_all_jobs(self) -> List[DocumentReference]:
    """Lists all running jobs

    Returns:
        jobs {List[DocumentReference]} -- List of all available jobs
    """
    return self.get_all_documents(Type._JOBS)

  def get_all_running(self) -> List[DocumentReference]:
    """Lists all running reports

    Lists all running reports

    Returns:
        runners {List[DocumentReference]} -- list of all running reports
    """
    return self.get_all_documents(Type._RUNNING)

  def get_all_documents(self, type: Type) -> List[DocumentReference]:
    """Lists all documents

    Lists all documents of a given Type

    Returns:
        runners {List[DocumentReference]} -- list of all documents
    """
    runners = []
    collection = self.client.collection(type.value).list_documents()
    for document in collection:
      runners.append(document)

    return runners

  def store_report_runner(self, runner: Dict[str, Any]) -> None:
    """Stores a running report

    Arguments:
        runner {Dict[str, Any]} -- store a running report definition
    """
    self.store_document(type=Type._RUNNING,
                        id=runner['report_id'], document=runner)

  def remove_report_runner(self, runner: str) -> None:
    """Removes a running report

    Delete a running report from the list of active reports

    Arguments:
        runner {Dict[str, Any]} -- [description]
    """
    self.delete_document(Type._RUNNING, runner['report_id'])

  def get_document(self, type: Type, id: str,
                   key: Optional[str]=None) -> Dict[str, Any]:
    """Loads a document (could be anything, 'type' identifies the root.)

    Load a document

    Arguments:
        type {Type} -- document type (document root in firestore)
        id {str} -- document id
        key: Optional(str): the document collection sub-key

    Returns:
        Dict[str, Any] -- stored configuration dictionary, or None
                          if not present
    """
    document = None

    if report:= self.client.document(f'{type}/{id}'):
      document = report.get().to_dict()

    return document.get(key) if key else document

  def store_document(self, type: Type, id: str,
                     document: Dict[str, Any]) -> None:
    """Stores a document.

    Store a document in Firestore. They're all stored by Type
    (DCM/DBM/SA360/ADH) and each one within the type is keyed by the
    appropriate report id.

    Arguments:
        type {Type} -- product
        id {str} -- report id
        report_data {Dict[str, Any]} -- report configuration
    """
    report = self.client.document(f'{type}/{id}')
    if report:
      report.delete()

    report.set(document)

  def update_document(self, type: Type, id: str,
                      new_data: Dict[str, Any]) -> None:
    """Updates a document.

    Update a document in Firestore. If the document is not already there, it
    will be created as a net-new document. If it is, it will be updated.

    Args:
        type (Type): the document type, which is the collection.
        id (str): the id of the document within the collection.
        new_data (Dict[str, Any]): the document content.
    """
    if collection := self.client.collection(f'{type}'):
      if document_ref := collection.document(document_id=id):
        if document_ref.get().exists:
          document_ref.update(new_data)
        else:
          document_ref.create(new_data)

  def delete_document(self, type: Type, id: str,
                      key: Optional[str]=None) -> None:
    """Deletes a document.

    This removes a document or partial document from the Firestore. If a key is
    supplied, then just that key is removed from the document. If no key is
    given, the entire document will be removed from the collection.

    Args:
        type (Type): the document type, which is the collection.
        id (str): the id of the document within the collection.
        key (str, optional): the key to remove. Defaults to None.
    """
    if collection := self.client.collection(f'{type}'):
      if document_ref := collection.document(document_id=id):
        if key:
          document_ref.update({ key: firestore.DELETE_FIELD })
        else:
          document_ref.delete()

  def list_documents(self, report_type: Type, key: str=None) -> List[str]:
    """Lists documents in a collection.

    List all the documents in the collection 'type'. If a key is give, list
    all the sub-documents of that key. For example:

    list_documents(Type.SA360_RPT) will show { '_reports', report_1, ... }
    list_documents(Type.SA360_RPT, '_reports') will return
      { 'holiday_2020', 'sa360_hourly_depleted', ...}

    Args:
        type (Type): the document type, which is the collection.
        key (str, optional): the sub-key. Defaults to None.

    Returns:
        List[str]: the list
    """
    documents = []
    collection = self.client.collection(f'{report_type}').list_documents()
    for document in collection:
      if key:
        if document.id == key:
          for _document in document.get().to_dict():
            documents.append(_document)
      else:
        documents.append(document.id)

    return documents
