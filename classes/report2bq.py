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

# Python logging
import logging
import os
import pprint
import re

# Class Imports
from classes import ReportFetcher
from classes.fetcher_factory import FetcherFactory
from classes.csv_helpers import CSVHelpers
from classes.dbm import DBM
from classes.dcm import DCM
from classes.sa360_v2 import SA360
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore
from classes.report_type import Type

from typing import Any, Mapping
from urllib.parse import unquote


class Report2BQ(object):
  def __init__(self, product: Type, email=None, project=None, 
    report_id=None,
    profile=None,
    sa360_url=None,
    force: bool=False, append: bool=False, infer_schema: bool=False,
    dest_project: str=None, dest_dataset: str='report2bq'):
    self.product = product

    self.force = force
    self.email = email
    self.append = append
    self.infer_schema = infer_schema
    
    self.report_id = report_id

    self.sa360_url = unquote(sa360_url) if sa360_url else None

    self.cm_profile = profile

    self.project = project

    self.dest_project = dest_project
    self.dest_dataset = dest_dataset

    self.firestore = Firestore(email=email, project=project)    


  def handle_report_fetcher(self, fetcher: ReportFetcher):
    # Get Latest Report
    report_object = fetcher.get_latest_report_file(self.report_id)

    if report_object:
      # Normalize Report Details
      report_data = fetcher.normalize_report_details(report_object)
      last_report = self.firestore.get_report_config(fetcher.report_type, self.report_id)

      if last_report:
        if report_data['last_updated'] == last_report['last_updated'] and not self.force:
          logging.info('No change: ignoring.')
          return

      csv_header, csv_types = fetcher.read_header(report_data)
      schema = CSVHelpers.create_table_schema(
        csv_header, 
        csv_types if self.infer_schema else None
      )

      # validate stored schema against current
      if self.append and not schema == last_report['schema']:
        logging.error('Cannot append with a different schema.')
        return

      report_data['schema'] = last_report['schema']
      report_data['email'] = self.email
      report_data['append'] = self.append

      if self.dest_project: report_data['dest_project'] = self.dest_project
      if self.dest_dataset: report_data['dest_dataset'] = self.dest_dataset
      self.firestore.store_report_config(fetcher.report_type, self.report_id, report_data)
      fetcher.stream_to_gcs(f'{self.project}-report2bq-upload', report_data)


  def handle_sa360(self):
    sa360 = SA360(project=self.project, email=self.email)

    logging.info(self.sa360_url)
    id = re.match(r'^.*rid=([0-9]+).*$', self.sa360_url).group(1)
    report_data = self.firestore.get_report_config(Type.SA360, id)

    if not report_data:
      # Create new report details structure
      report_data = {
        'id': id,
        'url': self.sa360_url
      }
      report_data['table_name'] = 'SA360_{id}'.format(id=id)
      report_data['email'] = self.email

    if self.dest_project: report_data['dest_project'] = self.dest_project
    if self.dest_dataset: report_data['dest_dataset'] = self.dest_dataset
    sa360.process(
      bucket='{project}-report2bq-upload'.format(project=self.project),
      report_details=report_data)
      
    self.firestore.store_report_config(Type.SA360, id, report_data)
  

  def run(self):
    if self.product in [ Type.DV360, Type.CM ]:
      fetcher = FetcherFactory.create_fetcher(self.product, email=self.email, project=self.project, profile=self.cm_profile)
      self.handle_report_fetcher(fetcher=fetcher)

    elif self.product == Type.SA360:
      self.handle_sa360()

    # if self.product == Type.DV360:
    #   fetcher = DBM(self.email, project=self.project)
    #   self.handle_report_fetcher(fetcher=fetcher)
    #   # self.handle_dv360_reports()

    # if self.product == Type.CM:
    #   fetcher = DCM(email=self.email, project=self.project, profile=self.cm_profile)
    #   self.handle_report_fetcher(fetcher=fetcher)
    #   # self.handle_cm_reports()
