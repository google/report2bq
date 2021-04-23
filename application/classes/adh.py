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

import logging
import pytz
import re

from datetime import datetime
from datetime import timedelta

from typing import Any, Dict

from classes import csv_helpers
from classes import decorators
from classes import discovery
from classes.credentials import Credentials
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore
from classes.report_type import Type
from classes.services import Service

from googleapiclient.discovery import Resource


class ADH(object):
  """Run ADH queries

  This class runs ADH queries. Where they output is determined by the query iteslf, in ADH. All
  we can specify here is the date range - and we do this by accepting a lookback window and doing
  "yesterday - lookback".

  TODO (davidharcombe@) ADH Query Parameters
  """

  def __init__(self,
    email: str, project: str, adh_customer: str,
    adh_query: str, api_key: str, days: int,
    dest_project: str=None, dest_dataset: str=None, dest_table: str=None,
    **unused) -> ADH:
    """Constructor

    Setus up the ADH helper

    Arguments:
        email (str):  authenticated user email (for the token)
        project (str):  GCP project
        adh_customer (str):  ADH customer id, 9-digit number, NO DASHES
        adh_query (str):  ADH query id
        api_key (str):  API Key (has to be set up in APIs and Libraries in GCP)
        days (int):  Lookback window (default: 60)
        dest_project (str):  target GCP project for results
        dest_dataset (str):  target BQ dataset for results
        dest_table (str):  target table override
    """
    self.email = email
    self.project = project
    self.adh_customer = adh_customer
    self.adh_query = adh_query
    self.api_key = api_key
    self.days = days
    self.dest_project = dest_project
    self.dest_dataset = dest_dataset
    self.dest_table = dest_table

  @decorators.lazy_property
  def credentials(self) -> Credentials:
    """Fetch the credentials on demand.

    Returns:
        Credentials: credentials
    """
    return Credentials(email=self.email, project=self.project)

  @decorators.lazy_property
  def storage(self) -> Cloud_Storage:
    """Fetch the GCS storage client on demand.

    Returns:
        Cloud_Storage: storage client
    """
    return Cloud_Storage()

  @decorators.lazy_property
  def firestore(self) -> Firestore:
    """Fetch the Firestore client on demand.

    Returns:
        Firestore: firestore client
    """
    return Firestore()

  def run(self, unattended: bool=True):
    """Run the ADH query

    Execute the ADH query, storing the run job result in Firestore. The data itself will be written
    to Big Query by ADH.
    Remember that ADH queries have many, many constraints so use this wisely: DON'T set up
    an hourly run - check with ADH.

    Keyword Arguments:
        unattended (bool):  run unattended. Unused, but there for compatibility (default: {True})
    """
    query_details = self.fetch_query_details()
    if query_details:
      report = {
        'id': self.adh_query,
        'details': query_details,
        'customer_id': self.adh_customer,
      }
      if self.dest_project:
        report['dest_project'] = self.dest_project

      if self.dest_dataset:
        report['dest_dataset'] = self.dest_dataset

      if self.dest_table:
        report['dest_table'] = self.dest_table

      query_details['table_name'] = \
        csv_helpers._sanitize_string(query_details['title'])

      self.firestore.store_document(type=Type.ADH, document=report,
                                    id=self.adh_query)

      result = self.run_query(report)
      self.firestore.update_document(type=Type.ADH, id=self.adh_query,
                                     new_data={'last_run': result})

      logging.info('Result: {result}'.format(result=result))

  @decorators.lazy_property
  def adh(self) -> Resource:
    """Create the ADH Service

    Use the discovery API to create the ADH service

    Returns:
        Resource: ADH service
    """
    adh_service = \
      discovery.get_service(service=Service.ADH,
        credentials=self.credentials, api_key=self.api_key)
    return adh_service

  def fetch_query_details(self) -> Dict[str, Any]:
    """Get the Query details

    Returns:
        Dict[str, Any]: [description]
    """
    service = self._get_adh_service()

    query_id = 'customers/{customer_id}/analysisQueries/{query_id}'.format(
      customer_id=self.adh_customer,
      query_id=self.adh_query)
    query = service.customers().analysisQueries().get(name=query_id).execute()

    return query

  def run_query(self, query_details: Dict[str, Any]) -> Dict[str, Any]:
    """Run the ADH query

    Arguments:
        query_details (Dict[str, Any]):  the details of the query job

    Returns:
        Dict[str, Any]: result of the query run directive
    """
    yesterday = datetime.now(tz=pytz.timezone('US/Eastern')) - timedelta(days=1)
    earliest = yesterday - timedelta(days=60)

    body = {
      "spec": {
        "startDate": {
          "year": earliest.year,
          "month": earliest.month,
          "day": earliest.day
        },
        "endDate": {
          "year": yesterday.year,
          "month": yesterday.month,
          "day": yesterday.day
        }
      },
      "destTable": '{project}.{dataset}.{table_name}'.format(
        project=query_details['dest_project'] if 'dest_project' in query_details else self.project,
        dataset=query_details['dest_dataset'] if 'dest_dataset' in query_details else 'adh_results',
        table_name=query_details['table_name']
      ),
      "customerId": query_details['customer_id']
    }
    result = self.adh.customers().analysisQueries().start(
      name=query_details['details']['name'], body=body).execute()

    return result
