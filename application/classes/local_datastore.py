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

import json

from classes import decorators
from classes.abstract_datastore import AbstractDatastore
from classes.report_type import Type

from typing import Any, Callable, Dict, List, Mapping, Optional

DATASTORE_FILE = 'datastore.json'


def persist(f: Callable) -> Any:
  def f_persist(*args: Mapping[str, Any], **kw: Mapping[str, Any]) -> Any:
    datastore = args[0].datastore
    try:
      return f(*args, **kw)
    finally:
      with open(DATASTORE_FILE, 'w') as storage:
        storage.write(json.dumps(datastore, indent=2))
  return f_persist

class LocalDatastore(AbstractDatastore):
  @decorators.lazy_property
  def datastore(self) -> Dict[str, Any]:
    try:
      with open(DATASTORE_FILE, 'r') as store:
        if data := store.read():
          return json.loads(data)
        else:
          return {}
    except FileNotFoundError:
      return {}

  def __init__(self, email: str=None, project: str=None) -> AbstractDatastore:
    self._project = project
    self._email = email

  def get_document(self, type: Type, id: str,
                   key: Optional[str]=None) -> Dict[str, Any]:
    """Fetches a document (could be anything, 'type' identifies the root.)

    Fetch a document

    Arguments:
        type (Type): document type (document root in firestore)
        id (str): document id
        key: Optional(str): the document collection sub-key

    Returns:
        Dict[str, Any]: stored configuration dictionary, or None
                          if not present
    """
    if document := self.datastore.get(type.value):
      parent = document.get(id)
      if parent:
        if key:
          value = parent.get(key)
          return {key: value} if value else None
        else:
          return {id: parent}
    else:
      return None

  @persist
  def store_document(self, type: Type, id: str,
                     document: Dict[str, Any]) -> None:
    """Stores a document.

    Store a document in Firestore. They're all stored by Type
    (DCM/DBM/SA360/ADH) and each one within the type is keyed by the
    appropriate report id.

    Args:
        type (Type): product
        id (str): report id
        report_data (Dict[str, Any]): report configuration
    """
    self.datastore.update({type.value: {id: document}})

  @persist
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
    root = self.datastore.get(type.value, {})
    if document := root.get(id):
      document.update(new_data)
    else:
      root[id] = new_data

  @persist
  def delete_document(self, type: Type, id: str,
                      key: Optional[str]=None) -> None:
    """Deletes a document.

    This removes a document or partial document from the Firestore. If a key is
    supplied, then just that key is removed from the document. If no key is
    given, the entire document will be removed from the collection. If neither
    key is present, nothing will happen.

    Args:
        type (Type): the document type, which is the collection.
        id (str): the id of the document within the collection.
        key (str, optional): the key to remove. Defaults to None.
    """
    try:
      if key:
        if doc := self.datastore.get(type.value, {}).get(id):
          doc.pop(key)
      else:
        self.datastore.get(type.value, {}).pop(id)

    except KeyError:
      None

  def list_documents(self, report_type: Type,
                     key: Optional[str]=None) -> List[str]:
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
    if docs := self.datastore.get(report_type.value):
      if key:
        if sub_docs := docs.get(key):
          keys = sub_docs.keys()
      else:
        keys = docs.keys()
    else:
      keys = None
    return keys

  def get_all_documents(self, type: Type) -> List[Dict[str, Any]]:
    """Lists all documents

    Lists all documents of a given Type

    Returns:
        documents (List[DocumentReference]): list of all documents
    """
    return self.list_documents(report_type=type)
