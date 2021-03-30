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

from googleapiclient import discovery as gdiscovery
from classes import credentials
from classes.services import Service
from classes import discovery
from classes.sa360_report_validation import sa360_validator_factory

import csv
import dataclasses
import dataclasses_json
import enum
import io
import json
import logging
import os
import pprint
import random
import stringcase
import uuid

# Class Imports
from contextlib import suppress
from datetime import datetime
from urllib.parse import unquote
from typing import Any, Dict, List, Tuple

from classes.firestore import Firestore
from classes.report_type import Type
from classes.scheduler import Scheduler
from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials


class Validity(enum.Enum):
    VALID = 'valid'
    INVALID = 'invalid'
    UNDEFINED = ''

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value

@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Validation(object):
    agency: str = None
    advertiser: str = None
    conversionMetric: Validity = Validity.UNDEFINED
    revenueMetric: bool = Validity.UNDEFINED

    @classmethod
    def keys(cls):
        return list(Validation.__dataclass_fields__.keys())

class SA360Manager(object):
  sa360 = None
  sa360_service = None
  scheduler = None
  saved_column_names = {}

  def manage(self, **kwargs):
    project = kwargs['project']
    email = kwargs.get('email')
    firestore = Firestore(project=project, email=email)
    if kwargs.get('api_key') is not None:
      os.environ['API_KEY'] = kwargs['API_KEY']

    if 'API_KEY' in os.environ: self.scheduler = Scheduler()

    if 'name' in kwargs:
      report_name = kwargs['name']
    elif 'file' in kwargs:
      report_name = kwargs['file'].split('/')[-1].split('.')[0]
    else:
      report_name = None

    args = {
      'report': report_name,
      'file': kwargs.get('file'),
      'firestore': firestore,
      'project': project,
      'email': email,
      **kwargs,
    }

    action = {
      'list': self.list_all,
      'show': self.show,
      'add': self.add,
      'delete': self.delete,
      'validate': self.validate,
      'install': self.install,
    }.get(kwargs['action'])

    if action:
      return action(**args)

    else:
      raise NotImplementedError()

  def list_all(
    self, firestore: Firestore, project: str, _print: bool=False,
    gcs_stored: bool=False, **unused):
    sa360_objects = firestore.list_documents(Type.SA360_RPT)
    reports = firestore.list_documents(Type.SA360_RPT, '_reports')
    results = []
    results.append(f'SA360 Dynamic Reports defined for project {project}')
    for report in reports:
      results.append(f'  {report}')
      for sa360_object in sa360_objects:
        if sa360_object.startswith(report):
          results.append(f'    {sa360_object}')
      self._output_results(
        results=results, project=project, email=None, file='report_list',
        gcs_stored=gcs_stored)

    if _print:
      for result in results:
        logging.info(result)

    return reports

  def show(
    self, firestore: Firestore, project: str, report: str, _print: bool=False,
    gcs_stored: bool=False, **unused):
    definition = firestore.get_document(Type.SA360_RPT, '_reports').get(report)
    results = [ l for l in json.dumps(definition, indent=2).splitlines() ]

    self._output_results(
      results=results, project=project, email=None, file=report,
      gcs_stored=gcs_stored)

    if _print:
      logging.info(f'SA360 Dynamic Report "{report}"')
      pprint.pprint(definition, indent=2, compact=False)

    return definition

  def add(
    self, firestore: Firestore, report: str, file: str, gcs_stored: bool=False,
    project: str = None, email: str = None, **unused):
    if gcs_stored:
      content = Cloud_Storage(
        project=project, email=email).fetch_file(
          bucket=f'{project}-report2bq-sa360-manager', file=file)
      cfg = json.loads(content)

    else:
      with open(file) as definition:
        cfg = json.loads(''.join(definition.readlines()))

    firestore.update_document(Type.SA360_RPT, '_reports', { report: cfg })

  def _read_file(self, file: str, gcs_stored: bool, project: str) -> str:
    if gcs_stored:
      logging.info(file)
      email = str(Cloud_Storage().fetch_file(
          bucket=f'{project}-report2bq-sa360-manager', file=file),
        encoding='utf-8').strip()

    else:
      with open(file, 'r') as _command_file:
        email = _command_file.readline().strip()

    if not email:
      logging.error('No email found, cannot access scheduler.')

    return email

  def delete(
    self, firestore: Firestore, project: str, report: str, email: str,
    gcs_stored: bool=False, file: str=None, **unused):
    firestore.delete_document(Type.SA360_RPT, '_reports', report)

    if email := \
      self._read_file(file=file, gcs_stored=gcs_stored, project=project):
      return

    if self.scheduler:
      args = {
        'action': 'list',
        'email': email,
        'project': project,
        'html': False,
      }

      # Disable all runners for the now deleted report
      runners = list(
        runner['name'].split('/')[-1] for runner in self.scheduler.process(args) if report in runner['name'])
      for runner in runners:
        args = {
          'action': 'disable',
          'email': email,
          'project': project,
          'job_id': runner,
        }
        self.scheduler.process(args)

  def validate(
    self, firestore: Firestore, project: str, email: str,
    file: str=None, gcs_stored: bool=False, **unused) -> None:
    sa360_report_definitions = \
      firestore.get_document(Type.SA360_RPT, '_reports')
    validation_results = []

    sa360_objects = \
      self._get_sa360_objects(
        firestore=firestore, file=file, gcs_stored=gcs_stored, project=project,
        email=email)

    for sa360_object in sa360_objects:
      if sa360_object == '_reports': continue
      creds = Credentials(project=project, email=sa360_object['email'])
      sa360_service = \
        discovery.get_service(service=Service.SA360, credentials=creds)

      (valid, validation) = \
        self._file_based(sa360_report_definitions, sa360_object, sa360_service)
      validation_results.append(validation)

    if validation_results:
      csv_output = f'{file}-validation.csv'
      if gcs_stored:
        csv_bytes = io.StringIO()
        writer = csv.DictWriter(
          csv_bytes, fieldnames=Validation.keys(), quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows([r.to_dict() for r in validation_results])
        Cloud_Storage(project=project, email=email).write_file(
          bucket=f'{project}-report2bq-sa360-manager',
          file=csv_output,
          data=csv_bytes.getvalue())

      else:
        with open(csv_output, 'w') as csv_file:
          writer = csv.DictWriter(
            csv_file, fieldnames=Validation.keys(), quoting=csv.QUOTE_ALL)
          writer.writeheader()
          writer.writerows([r.to_dict() for r in validation_results])

  def _get_sa360_objects(
    self, firestore: Firestore, file: str, project: str, email: str,
    gcs_stored: bool=False) -> List[Dict[str, Any]]:
    if file:
      if gcs_stored:
        content = Cloud_Storage(
          project=project, email=email).fetch_file(
            bucket=f'{project}-report2bq-sa360-manager', file=file)
        sa360_objects = json.loads(content)
      else:
        with open(file) as rpt:
          sa360_objects = json.loads(''.join(rpt.readlines()))

    else:
      sa360_objects = firestore.list_documents(Type.SA360_RPT)

    return sa360_objects

  def _file_based(
    self, sa360_report_definitions: Dict[str, Any],
    report: Dict[str, Any],
    sa360_service: gdiscovery.Resource) -> Tuple[bool, Dict[str, Any]]:
    logging.info(
      'Validating %s (%s/%s) on report %s', report.get("agencyName", "-"),
      report["AgencyId"], report["AdvertiserId"], report["report"])

    target_report = sa360_report_definitions[report['report']]
    validator = \
      sa360_validator_factory.SA360ValidatorFactory().get_validator(
        report_type=target_report['report']['reportType'],
        sa360_service=sa360_service,
        agency=report['AgencyId'], advertiser=report['AdvertiserId'])
    report_custom_columns = \
      [column['name'] for column in target_report['parameters'] if 'is_list' in column]
    valid = True
    validation = Validation(report['AgencyId'], report['AdvertiserId'])

    for report_custom_column in report_custom_columns:
      if report[report_custom_column]:
        (valid_column, name) = validator.validate(report[report_custom_column])
        valid = valid and valid_column
        validity = Validity.UNDEFINED
        if not valid:
          validity = Validity.INVALID
        elif report[report_custom_column]['value']:
          validity = Validity.VALID

        setattr(
          validation, stringcase.camelcase(report_custom_column), validity)
        if not valid_column and name:
          logging.info(
            f'  Field {report_custom_column} - {report[report_custom_column]}: '
            f'{valid_column}, did you mean "{name}"')
        else:
          logging.info(
            f'  Field {report_custom_column} - {report[report_custom_column]}: '
            f'{valid_column}')

    if len(set(report_custom_columns)) != len(report_custom_columns):
      valid = False

    return (valid, validation)

  def install(self, firestore: Firestore, project: str, email: str,
    file: str=None, gcs_stored: bool=False, **unused) -> None:
    scheduler = Scheduler()
    results = []
    random.seed(uuid.uuid4())


    runners = self._get_sa360_objects(firestore, file, project, email, gcs_stored)
    sa360_report_definitions = firestore.get_document(Type.SA360_RPT, '_reports')

    for runner in runners:
      id = f"{runner['report']}_{runner['AgencyId']}_{runner['AdvertiserId']}"

      creds = Credentials(project=project, email=runner['email'])
      sa360_service = \
        discovery.get_service(service=Service.SA360, credentials=creds)
      (valid, validity) = self._file_based(
        sa360_report_definitions=sa360_report_definitions,
        report=runner, sa360_service=sa360_service)

      if valid:
        logging.info('Valid report: %s', id)

        old_runner = firestore.get_document(type=Type.SA360_RPT, id=id)
        if old_runner:
          for k in runner.keys():
            if k == 'minute': continue
            if k in old_runner and old_runner[k] != runner[k]:
              break
          results.append(f'{id} - Identical report present. No action taken.')
          continue

        firestore.store_document(Type.SA360_RPT, f'{id}', runner)
        job_id = f"run-{Type.SA360_RPT}-{id}"

        args = {
          'action': 'get',
          'email': runner['email'],
          'project': f'{project}',
          'job_id': job_id,
        }

        try:
          present, job = scheduler.process(args)

        except Exception as e:
          logging.error(e)
          results.append(f'{id} - Check if already defined failed: {e}')
          continue

        if present:
          args = {
            'action': 'delete',
            'email': runner['email'],
            'project': f'{project}',
            'job_id': job_id,
          }
          try:
            scheduler.process(args)

          except Exception as e:
            logging.error(e)
            results.append(f'{id} - Already present but delete failed: {e}')
            continue

        args = {
          'action': 'create',
          'email': runner['email'],
          'project': f'{project}',
          'force': False,
          'infer_schema': runner.get('infer_schema', False),
          'append': runner.get('append', False),
          'sa360_id': id,
          'description':
            runner['description'] if 'description' in runner else (
              f'{runner["title"] if "title" in runner else runner["report"]}: '
              f'{runner["agencyName"]}/{runner["advertiserName"]}'),
          'dest_dataset': runner.get('dest_dataset', 'report2bq'),
          'minute': runner.get('minute', random.randrange(0, 59)),
          'hour': runner.get('hour', '*')
        }

        try:
          scheduler.process(args)
          results.append(f'{id} - Valid and installed.')


        except Exception as e:
          logging.error(e)
          results.append(f'{id} - Failed to create: {e}')

      else:
        logging.info('Invalid report: %s', id)
        results.append(f'{id} - Validation failed: {validity}')

    if results:
      self._output_results(
        results=results, project=project, email=email, gcs_stored=gcs_stored,
        file=file)

  def _output_results(
    self, results: List[str], project: str, email: str, file: str=None,
    gcs_stored: bool=False) -> None:
    def _send():
      for result in results:
        print(result, file=outfile)

    output_name = f'{file}.results'
    if gcs_stored:
      outfile = io.StringIO()
      _send()

      Cloud_Storage(project=project, email=email).write_file(
        bucket=f'{project}-report2bq-sa360-manager',
        file=output_name,
        data=outfile.getvalue())

    else:
      with open(output_name, 'w') as outfile:
        _send()
