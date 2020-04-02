"""
Copyright 2018 Google LLC

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
import pprint
import re

# Class Imports
from classes.csv_helpers import CSVHelpers
from classes.dbm import DBM
from classes.dcm import DCM
from classes.sa360_v2 import SA360
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore
from classes.report_type import Type
from urllib.parse import unquote


class Report2BQ(object):
  def __init__(self, list_reports=False, dv360=False, cm=False, force=False, rebuild_schema=False,
               dv360_ids=None, cm_ids=None, profile=None, account_id=None, superuser=False, 
               email=None, in_cloud=True, append=False, project=None, sa360_url=None, sa360=False):
    self.list_reports = list_reports
    self.rebuild_schema = rebuild_schema
    self.force = force
    self.email = email
    self.append = append
    
    self.dv360 = dv360
    self.dv360_ids = dv360_ids

    self.sa360_url = unquote(sa360_url) if sa360_url else None
    self.sa360 = sa360

    self.cm = cm
    self.cm_ids = cm_ids
    self.cm_profile = profile
    self.account_id = account_id
    self.superuser = superuser
    self.in_cloud = in_cloud
    self.project = project

    self.storage = Cloud_Storage(in_cloud=in_cloud, email=email, project=project)
    self.firestore = Firestore(in_cloud=in_cloud, email=email, project=project)    


  def handle_dv360_reports(self):
    dbm = DBM(self.email, project=self.project)

    for id in self.dv360_ids:
      # Get Latest Report
      report_object = dbm.get_latest_report_file(id)

      if report_object:
        # Normalize Report Details
        report_data = dbm.normalize_report_details(report_object)
        last_report = self.firestore.get_report_config(Type.DBM, id)

        if last_report:
          if report_data['last_updated'] == last_report['last_updated'] and not self.force:
            print('No change: ignoring.')
            continue

        if not last_report or self.rebuild_schema:
          # Store Report Details
          # csv_header = self.storage.read_first_line(report_data)
          csv_header, _ = dbm.read_header(report_data, self.storage)
          schema = CSVHelpers.create_table_schema(csv_header)
          report_data['schema'] = schema

        else:
          report_data['schema'] = last_report['schema']

        report_data['email'] = self.email
        report_data['append'] = self.append
        self.firestore.store_report_config(Type.DBM, id, report_data)
        Cloud_Storage.copy_to_gcs('{project}-report2bq-upload'.format(project=self.project), report_data)


  def handle_cm_reports(self):
    # pprint.pprint(dcm.get_user_profiles())
    dcm = DCM(superuser=self.superuser, email=self.email, project=self.project)

    for id in self.cm_ids:
      # Get Latest Report
      report_object = dcm.get_latest_report_file(self.cm_profile, id, self.account_id)

      if report_object:
        # Normalize Report Details
        report_data = dcm.normalize_report_details(report_object)
        last_report = self.firestore.get_report_config(Type.DCM, id)

        if last_report:
          if report_data['last_updated'] == last_report['last_updated'] and not self.force:
            print('No change: ignoring.')
            continue

        if not last_report or self.rebuild_schema:
          # Store Report Details
          csv_header, _ = dcm.read_header(report_data)
          schema = CSVHelpers.create_table_schema(csv_header)
          report_data['schema'] = schema

        else:
          report_data['schema'] = last_report['schema']

        report_data['email'] = self.email
        report_data['append'] = self.append
        self.firestore.store_report_config(Type.DCM, id, report_data)
        dcm._stream_to_gcs(bucket='{project}-report2bq-upload'.format(project=self.project), report_data=report_data, storage=self.storage)


  def handle_sa360(self):
    sa360 = SA360(project=self.project, email=self.email, _storage=self.storage)

    logging.info(self.sa360_url)
    id = re.match(r'^.*rid=([0-9]+).*$', self.sa360_url).group(1)
    report_details = self.firestore.get_report_config(Type.SA360, id)

    if report_details and not self.rebuild_schema:
      report_details['url']

    else:
      # Create new report details structure
      report_details = {
        'id': id,
        'url': self.sa360_url
      }
      report_details['table_name'] = 'SA360_{id}'.format(id=id)

    sa360.process(
      bucket='{project}-report2bq-upload'.format(project=self.project),
      report_details=report_details)
      
    self.firestore.store_report_config(Type.SA360, id, report_details)
  

  def list_all_reports(self):
    if self.dv360:
      print('DV360')
      print('~~~~~')
      reports = self.firestore.get_all_reports(Type.DV360)
      if reports:
        [print('Report: {id} ({name}) - last updated {update}.'.format(id=report['id'], name=report['name'], update=report['last_updated'])) for report in reports]
      else:
        print('No DV360 reports stored.')
      print()

    if self.cm:
      print('Campaign Manager')
      print('~~~~~~~~~~~~~~~~')
      reports = self.firestore.get_all_reports(Type.CM)
      if reports:
        [print('Report: {id} ({name}) - last updated {update}.'.format(id=report['id'], name=report['name'], update=report['last_updated'])) for report in reports]
      else:
        print('No CM reports stored.')
      print()


  def run(self):
    if self.list_reports:
      self.list_all_reports()

    if self.dv360:
      self.handle_dv360_reports()

    if self.cm:
      self.handle_cm_reports()

    if self.sa360:
      self.handle_sa360()
