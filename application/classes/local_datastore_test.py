# Copyright 2021 Google LLC
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

import json
import unittest
from unittest import mock

from classes import local_datastore
from classes.report_type import Type

from copy import deepcopy
from io import StringIO
from typing import Any, Callable, Dict, Mapping

MASTER_CONFIG = {
    "administration": {
        "auth": {
            "api_key": "api_key",
            "bHVrZUBza3l3YWxrZXIuY29t": {
                "access_token": "access_token",
                "refresh_token": "refresh_token",
                "_key": "luke@skywalker.com"
            },
        },
    },
}

CLASS_UNDER_TEST = 'classes.local_datastore'


class LocalDatastoreTest(unittest.TestCase):
  def setUp(self):
    self.open = mock.mock_open(read_data=json.dumps(MASTER_CONFIG))

  def test_get_document_with_key(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()
      self.assertEqual({'api_key': 'api_key'},
                       datastore.get_document(Type._ADMIN, 'auth', 'api_key'))

  def test_get_document_without_key(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()
      self.assertEqual(MASTER_CONFIG.get(Type._ADMIN.value, 'auth'),
                       datastore.get_document(Type._ADMIN, 'auth'))

  def test_get_document_missing_type(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()
      self.assertEqual(None, datastore.get_document(Type.DV360, '10011'))

  def test_get_document_missing_id(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()
      self.assertEqual(None, datastore.get_document(Type._ADMIN, '10011'))

  def test_get_document_missing_key(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()
      self.assertEqual(None, datastore.get_document(Type._ADMIN, 'auth', 'foo'))

  def test_store_new_document(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()
      datastore.store_document(Type.DV360, id='0000', document={'id': '0000'})

      expected = deepcopy(MASTER_CONFIG)
      expected.update({'dv360': {'0000': {'id': '0000'}}})
      self.open.assert_called_with('datastore.json', 'w')
      self.open().write.assert_called_with(json.dumps(expected, indent=2))

  def test_list_documents_all(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()

      _docs = datastore.list_documents(Type._ADMIN)
      expected = MASTER_CONFIG.get(Type._ADMIN.value)
      self.assertEqual(expected.keys(), _docs)

  def test_list_documents_auth(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()

      _docs = datastore.list_documents(Type._ADMIN, 'auth')
      expected = MASTER_CONFIG.get(Type._ADMIN.value).get('auth')
      self.assertEqual(expected.keys(), _docs)

  def test_list_documents_none(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()

      _docs = datastore.list_documents(Type.DV360)
      self.assertEqual(None, _docs)

  def test_get_all_documents(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()

      _docs = datastore.get_all_documents(Type._ADMIN)
      expected = MASTER_CONFIG.get(Type._ADMIN.value).keys()
      self.assertEqual(expected, _docs)

  def test_delete_document_collection(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore = local_datastore.LocalDatastore()

      datastore.delete_document(type=Type._ADMIN, id='auth')
      self.assertEqual({'administration': {}}, datastore.datastore)

  def test_delete_document_key(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore=local_datastore.LocalDatastore()

      datastore.delete_document(
          type = Type._ADMIN, id = 'auth', key = 'api_key')
      self.assertEqual({'administration': {
                          'auth': {'bHVrZUBza3l3YWxrZXIuY29t': {
                              '_key': 'luke@skywalker.com',
                              'access_token': 'access_token',
                              'refresh_token': 'refresh_token'}}}},
                       datastore.datastore)

  def test_delete_document_key_missing(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore=local_datastore.LocalDatastore()

      datastore.delete_document(
          type = Type._ADMIN, id = 'auth', key = 'foo')
      self.assertEqual(MASTER_CONFIG,
                       datastore.datastore)

  def test_update_document_existing(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore=local_datastore.LocalDatastore()

      expected = {'api_key': 'new api key'}
      datastore.update_document(type=Type._ADMIN, id='auth',
                                new_data=expected)
      self.open().write.assert_called_once()
      self.assertEqual(expected.get('api_key'),
          datastore.datastore.get(Type._ADMIN.value).get('auth').get('api_key'))

  def test_update_document_new(self):
    with mock.patch(f'{CLASS_UNDER_TEST}.open', self.open):
      datastore=local_datastore.LocalDatastore()

      expected = {'new_api_key': 'new api key'}
      datastore.update_document(type=Type._ADMIN, id='auth',
                                new_data=expected)
      self.open().write.assert_called_once()
      self.assertEqual('new api key',
          datastore.datastore.get(Type._ADMIN.value).get('auth').get('new_api_key'))
      self.assertEqual('api_key',
          datastore.datastore.get(Type._ADMIN.value).get('auth').get('api_key'))
