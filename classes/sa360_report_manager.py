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

from classes.services import Service
from classes.discovery import DiscoverService
from classes.sa360_v2 import SA360
from classes.sa360_reports import SA360ReportTemplate
import json
import logging
import os
import pprint

# Class Imports
from contextlib import suppress
from datetime import datetime
from urllib.parse import unquote
from typing import List

from classes.firestore import Firestore
from classes.report_type import Type
from classes.scheduler import Scheduler


class SA360Manager(object):
  sa360 = None
  sa360_service = None
  scheduler = None
  saved_column_names = {}
  
  def manage(self, **kwargs):
    project = kwargs['project']
    email = kwargs['email']
    firestore = Firestore(project=project, email=email)
    if api_key := kwargs['api_key']: os.environ['API_KEY'] = api_key

    if 'API_KEY' in os.environ: self.scheduler = Scheduler()

    args = {
      'report': kwargs.get('name', kwargs.get('file').split('/')[-1].split('.')[0] if kwargs.get('file') else None),
      'file': kwargs.get('file'),
      'firestore': firestore,
      'project': kwargs['project'],
      'email': kwargs['email'],
      **kwargs,
    }

    action = {
      'list': self.list_all,
      'show': self.show,
      'add': self.add,
      'delete': self.delete,
      'validate': self.validate,
    }.get(kwargs['action'])
    
    if action:
      self.sa360 = SA360(email, project)
      return action(**args)

    else:
      raise NotImplementedError()

  def list_all(self, firestore: Firestore, project: str, _print: bool=False, **unused): 
    sa360_objects = firestore.list_documents(Type.SA360_RPT)
    reports = firestore.list_documents(Type.SA360_RPT, '_reports')
    if _print:
      print(f'SA360 Dynamic Reports defined for project {project}')
      print()
      for report in reports:
        print(f'  {report}')
        for sa360_object in sa360_objects:
          if sa360_object.startswith(report):
            print(f'    {sa360_object}')

    return reports

  def show(self, firestore: Firestore, report: str, _print: bool=False, **unused):
    definition = firestore.get_document(Type.SA360_RPT, '_reports').get(report)
    if _print:
      print(f'SA360 Dynamic Report "{report}"')
      print()
      pprint.pprint(definition, indent=2, compact=False)

    return definition

  def add(self, firestore: Firestore, report: str, file: str, **unused): 
    with open(file) as definition:
      cfg = json.loads(''.join(definition.readlines()))
      Firestore().update_document(Type.SA360_RPT, '_reports', { report: cfg })

  def delete(self, firestore: Firestore, project: str, report: str, email: str, **unused): 
    firestore.delete_document(Type.SA360_RPT, '_reports', report)
    scheduler = Scheduler()
    args = {
      'action': 'list',
      'email': email,
      'project': project,
      'html': False,
    }

    # Disable all runners for the now deleted report
    runners = list(runner['name'].split('/')[-1] for runner in scheduler.process(args) if report in runner['name'])
    for runner in runners:
      args = {
        'action': 'disable',
        'email': None,
        'project': project,
        'job_id': runner,
      }
      scheduler.process(args)

  def validate(self, firestore: Firestore, project: str, _print: bool=False, file=None, **unused):
    sa360_report_definitions = firestore.get_document(Type.SA360_RPT, '_reports')

    if file:
      with open(file) as rpt:
        sa360_objects = json.loads(''.join(rpt.readlines()))
    else:
      sa360_objects = firestore.list_documents(Type.SA360_RPT)

    for sa360_object in sa360_objects:
      if sa360_object == '_reports': continue

      if file:
        self._file_based(project, sa360_report_definitions, sa360_object)

      else:
        self._firestore_based(project, firestore, sa360_report_definitions, sa360_object)

  def _file_based(self, project, sa360_report_definitions, report) -> bool:
    print(f'Validating {report.get("agencyName", "-")} ({report["AgencyId"]}/{report["AdvertiserId"]}):')
    target_report = sa360_report_definitions[report['report']]
    custom_columns = self.list_custom_columns(project, report['AgencyId'], report['AdvertiserId'])
    report_custom_columns = [column['name'] for column in target_report['parameters'] if 'ordinal' in column]
    valid = True
    for report_custom_column in report_custom_columns:
      if report[report_custom_column]:
        valid = valid and report[report_custom_column] in custom_columns
        print(f'  Checking {report_custom_column}: {report[report_custom_column]}: {valid}')

    if not valid:
      print('  Available custom columns for this agency/advertiser pair:')
      for custom_column in custom_columns: 
        print(f'    "{custom_column}"')

    return valid

  def _firestore_based(self, project, firestore, sa360_report_definitions, sa360_object) -> bool: 
    print(f'Validating {sa360_object}:')
    report = firestore.get_document(Type.SA360_RPT, sa360_object)
    target_report = sa360_report_definitions[report['report']]
    custom_columns = self.list_custom_columns(project, report['AgencyId'], report['AdvertiserId'])
    report_custom_columns = [column['name'] for column in target_report['parameters'] if 'ordinal' in column]
    valid = True
    for report_custom_column in report_custom_columns:
      if report[report_custom_column]:
        valid = valid and report[report_custom_column] in custom_columns
        print(f'  Checking {report_custom_column}: {report[report_custom_column]}: {valid}')

    if 'API_KEY' in os.environ:
      args = {
        'action': 'enable' if valid else 'disable',
        'email': self.sa360.email,
        'project': project,
        'job_id': f'run-sa360_report-{sa360_object}',
      }
      self.scheduler.process(args)

    if not valid:
      print('  Available custom columns for this agency/advertiser pair:')
      for custom_column in custom_columns:
        print(f'    "{custom_column}"')

    return valid

  def list_custom_columns(self, project: str, agency: int, advertiser: int) -> List[str]:
    if saved_columns := self.saved_column_names.get((agency, advertiser)):
      return saved_columns

    if not self.sa360_service:
      self.sa360_service = DiscoverService.get_service(Service.SA360, self.sa360.creds)

    request = self.sa360_service.savedColumns().list(agencyId=agency, advertiserId=advertiser)
    response = request.execute()

    if 'items' in response:
      saved_columns = [item['savedColumnName'] for item in response['items']]
      self.saved_column_names[(agency, advertiser)] = saved_columns
    else:
      saved_columns = ['--- No custom columns found ---']

    return saved_columns
