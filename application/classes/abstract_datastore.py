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

from typing import Any, Dict, List, Optional

from classes.report_type import Type

from google.cloud import bigquery


class AbstractDatastore(object):
  """Abstract Datastore.

  This is the Datastore contract to be fufilled by any storage method. It
  contains the functions to be implemented by the concrete versions, as well as
  helpers (that are used throughought the system) which simply recall the more
  generic functions - for example 'remove_report_runner(id)' is the same as
  'delete_document(Type._RUNNER, id)' but in the context of where it is used
  is clearer than the latter.

  All unimplemented functions raise a NotImplementedError() rather than
  simply 'pass'.

  TODO: investigate removing the helpers in the code to slim this down.
        -- David Harcombe, 2021-04-23
  """

  # #########################################
  # To BE IMPLEMENTED
  # #########################################
  def get_document(self, type: Type, id: str,
                   key: Optional[str]=None) -> Dict[str, Any]:
    """Fetches a document (could be anything, 'type' identifies the root.)

    Fetch a document

    Arguments:
        type (Type): document type (document root in firestore)
        id (str): document id
        key: Optional(str): the document collection sub-key

    Returns:
        Dict[str, Any] -- stored configuration dictionary, or None
                          if not present
    """
    raise NotImplementedError('Must be implemented by child class.')

  def store_document(self, type: Type, id: str,
                     document: Dict[str, Any]) -> None:
    """Stores a document.

    Store a document in Firestore. They're all stored by Type
    (DCM/DBM/SA360/ADH) and each one within the type is keyed by the
    appropriate report id.

    Arguments:
        type (Type): product
        id (str): report id
        report_data (Dict[str, Any]): report configuration
    """
    raise NotImplementedError('Must be implemented by child class.')

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
    raise NotImplementedError('Must be implemented by child class.')

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
    raise NotImplementedError('Must be implemented by child class.')

  def get_all_documents(self, type: Type) -> List[Dict[str, Any]]:
    """Fetches all documents

    Fetches all documents of a given Type.

    Args:
        type (Type): the document type, which is the collection.

    Returns:
        runners (List[Dict[str, Any]]): contents of all documents
    """
    raise NotImplementedError('Must be implemented by child class.')

  # #########################################
  # HELPERS
  # #########################################
  def get_report_config(self, type: Type, id: str) -> Dict[str, Any]:
    """Loads a config.

    Arguments:
        type (Type): report type
        id (str): report id

    Returns:
        config (Dict[str, Any]): stored configuration dictionary
    """
    return self.get_document(type, id)

  def store_report_config(self, type: Type, id: str,
                          report_data: Dict[str, Any]) -> None:
    """Stores a config

    Store a report's config in Firestore. They're all stored by Type
    (DCM/DBM/SA360/ADH) and each one within the type is keyed by the
    appropriate report id.

    Arguments:
        type (Type): product
        id (str): report id
        report_data (Dict[str, Any]): report configuration
    """
    self.store_document(type=type, id=id, document=report_data)

  def get_all_reports(self, type: Type) -> List[Dict[str, Any]]:
    """Lists all reports

    List all defined reports for a specific product.

    Arguments:
        type (Type): product type

    Returns:
        reports (List[Dict[str, Any]]): list of all reports for a given project
    """
    raise self.get_all_documents(type=type)

  def store_import_job_details(self, report_id: int,
                               job: bigquery.LoadJob) -> None:
    """Saves a BQ Import job in Firestore

    Arguments:
        report_id (int): [description]
        job (bigquery.LoadJob): [description]
    """
    self.store_document(Type._JOBS, report_id, job.to_api_repr())

  def mark_import_job_complete(self, report_id: int,
                               job: bigquery.LoadJob) -> None:
    """Marks a BQ Import job in Firestore done

    Moves an import job from 'jobs/' to 'jobs-completed'.

    Arguments:
        report_id (int): [description]
        job (bigquery.LoadJob): [description]
    """
    self.delete_document(Type._JOBS, report_id)
    self.store_document(Type._COMPLETED, report_id, job.to_api_repr())

  def get_all_jobs(self) -> List[Dict[str, Any]]:
    """Lists all running jobs

    Returns:
        jobs (List[DocumentReference]): List of all available jobs
    """
    return self.get_all_documents(Type._JOBS)

  def get_all_running(self) -> List[Dict[str, Any]]:
    """Lists all running reports

    Lists all running reports

    Returns:
        runners (List[DocumentReference]): list of all running reports
    """
    return self.get_all_documents(Type._RUNNING)

  def store_report_runner(self, runner: Dict[str, Any]) -> None:
    """Stores a running report

    Arguments:
        runner (Dict[str, Any]): store a running report definition
    """
    self.store_document(type=Type._RUNNING,
                        id=runner['report_id'], document=runner)

  def remove_report_runner(self, runner: str) -> None:
    """Removes a running report

    Delete a running report from the list of active reports

    Arguments:
        runner (Dict[str, Any]): [description]
    """
    self.delete_document(Type._RUNNING, runner)
