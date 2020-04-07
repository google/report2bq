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
import pytz
import re

from datetime import datetime 
from datetime import timedelta

from typing import Dict, List, Any

# Class Imports
from classes.credentials import Credentials
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore
from classes.report_type import Type

from googleapiclient.discovery import build, Resource


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
    dest_project: str=None, dest_dataset: str=None):
    """Constructor

    Setus up the ADH helper
    
    Arguments:
        email {str} -- authenticated user email (for the token)
        project {str} -- GCP project
        adh_customer {str} -- ADH customer id, 9-digit number, NO DASHES
        adh_query {str} -- ADH query id
        api_key {str} -- API Key (has to be set up in APIs and Libraries in GCP)
        days {int} -- Lookback window (default: 60)
        dest_project {str} -- target GCP project for results
        dest_dataset {str} -- target BQ dataset for results
    """
    self.email = email
    self.project = project
    self.adh_customer = adh_customer
    self.adh_query = adh_query
    self.api_key = api_key
    self.days = days
    self.dest_project = dest_project
    self.dest_dataset = dest_dataset

    self.credentials = Credentials(email=email, project=project)
    self.storage = Cloud_Storage(email=email, project=project)
    self.firestore = Firestore(email=email, project=project)


  def run(self, unattended: bool=True):
    """Run the ADH query
    
    Execute the ADH query, storing the run job result in Firestore. The data itself will be written
    to Big Query by ADH.
    Remember that ADH queries have many, many constraints so use this wisely: DON'T set up
    an hourly run - check with ADH.

    Keyword Arguments:
        unattended {bool} -- run unattended. Unused, but there for compatibility (default: {True})
    """
    query_details = self.fetch_query_details()
    if query_details:
      report = {
        'id': self.adh_query,
        'details': query_details,
        'customer_id': self.adh_customer,
        'table_name': self._sanitize_string(query_details['title']),
      }
      if self.dest_project:
        report['dest_project'] = self.dest_project

      if self.dest_dataset:
        report['dest_dataset'] = self.dest_dataset

      self.firestore.store_report_config(
        type=Type.ADH,
        report_data=report,
        id=self.adh_query)
    
      result = self.run_query(report)
      report['last_run'] = result
      self.firestore.store_report_config(
        type=Type.ADH,
        report_data=report,
        id=self.adh_query)

      logging.info('Result: {result}'.format(result=result))


  def _get_adh_service(self) -> Resource:
    """Create the ADH Service

    Use the discovery API to create the ADH service
    
    Returns:
        Resource -- ADH service
    """
    adh_service = build(
      'AdsDataHub', 'v1', 
      credentials=self.credentials.creds, developerKey=self.api_key,
      cache_discovery=False
    )
    return adh_service


  def _sanitize_string(self, original: str) -> str:
    """Sanitize Strings

    Convert any non alphanumeric into an '_' as per BQ requirements
    
    Arguments:
        original {str} -- 
    
    Returns:
        str -- 
    """
    return re.sub('[^a-zA-Z0-9,]', '_', original)

    
  def fetch_query_details(self) -> Dict[str, Any]:
    """Get the Query details
    
    Returns:
        Dict[str, Any] -- [description]
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
        query_details {Dict[str, Any]} -- the details of the query job
    
    Returns:
        Dict[str, Any] -- result of the query run directive
    """
    service = self._get_adh_service()

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
    result = service.customers().analysisQueries().start(
      name=query_details['details']['name'], body=body).execute()

    return result