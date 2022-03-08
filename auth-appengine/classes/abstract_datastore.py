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
  """
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
