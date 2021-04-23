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

from classes import decorators
from classes.abstract_datastore import AbstractDatastore
from classes.report_type import Type

from google.cloud import firestore


class Firestore(AbstractDatastore):
  @decorators.lazy_property
  def client(self) -> Any:
    """The datastore client."""
    return firestore.Client()

  def __init__(self, email: str=None, project: str=None) -> AbstractDatastore:
    self._project = project
    self._email = email

  def get_all_documents(self, type: Type) -> List[Dict[str, Any]]:
    """Lists all documents

    Lists all documents of a given Type

    Returns:
        documents (List[Dict[str, Any]]): list of all documents
    """
    documents = []
    collection = self.client.collection(type.value).list_documents()
    for document in collection:
      documents.append(document.get().to_dict())

    return documents

  def get_document(self, type: Type, id: str,
                   key: Optional[str]=None) -> Dict[str, Any]:
    """Loads a document (could be anything, 'type' identifies the root.)

    Load a document

    Arguments:
        type (Type): document type (document root in firestore)
        id (str): document id
        key: Optional(str): the document collection sub-key

    Returns:
        Dict[str, Any]: stored configuration dictionary, or None
                          if not present
    """
    document = None

    if report:= self.client.document(f'{type}/{id}'):
      document = report.get().to_dict()

    return document.get(key) if key and document else document

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
