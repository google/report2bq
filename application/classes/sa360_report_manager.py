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

import csv
import dataclasses
import enum
import io
import json
import logging as log
import os
import random
import stringcase
import uuid

from typing import Any, Dict, List, Tuple, Union

import dataclasses_json
import stringcase
from auth.credentials import Credentials
from auth.datastore.secret_manager import SecretManager
from google.cloud import bigquery
from googleapiclient import discovery as gdiscovery
from service_framework import service_builder
from classes import gmail

from classes.cloud_storage import Cloud_Storage
from classes.query.report_manager import ActiveAccounts, ManagerUpdate
from classes.report_manager import (ManagerConfiguration, ManagerType,
                                    ReportManager)
from classes.report_type import Type
from classes.sa360_job import SA360Job, SA360ReportMetric
from classes.sa360_report_validation import sa360_validator_factory

from google.cloud import logging

logging_client = logging.Client()
logging_client.setup_logging()


class Validity(enum.Enum):
  VALID = 'valid'
  INVALID = 'invalid'
  UNDEFINED = ''

  def __repr__(self) -> str:
    return self.value

  def __str__(self) -> str:
    return self.value

  def to_json(self) -> str:
    return self.value


@dataclasses_json.dataclass_json
@dataclasses.dataclass
class Validation(object):
  agency: str = None
  advertiser: str = None
  conversionMetric: Validity = Validity.UNDEFINED
  revenueMetric: Validity = Validity.UNDEFINED
  customColumns: List[str] = None

  @classmethod
  def keys(cls):
    return list(Validation.__dataclass_fields__.keys())


class SA360Manager(ReportManager):
  report_type = Type.SA360_RPT
  sa360 = None
  sa360_service = None
  saved_column_names = {}
  actions = {
      'list',
      'show',
      'add',
      'delete',
      'validate',
      'install',
      'maddie',
      'pcrawf'
  }

  def manage(self, **kwargs) -> Any:
    project = kwargs['project']
    email = kwargs.get('email')
    self.bucket = f'{project}-report2bq-sa360-manager'

    if kwargs.get('api_key') is not None:
      os.environ['API_KEY'] = kwargs['API_KEY']

    if 'name' in kwargs:
      report_name = kwargs['name']
    elif 'file' in kwargs:
      report_name = kwargs['file'].split('/')[-1].split('.')[0]
    else:
      report_name = None

    source = None
    if kwargs.get('file') and kwargs.get('gcs_stored'):
      source = ManagerType.FILE_GCS
    elif kwargs.get('file'):
      source = ManagerType.FILE_LOCAL
    else:
      source = ManagerType.BIG_QUERY

    config: ManagerConfiguration = ManagerConfiguration(
        type=source,
        project=project,
        email=email,
        file=kwargs.get('file'),
        table='sa360_definition'
    )

    args = {
        'report': report_name,
        'config': config,
        **kwargs,
    }

    return self._get_action(kwargs.get('action'))(**args)

  def pcrawf(self, config: ManagerConfiguration, **unused) -> None:
    def create_service():
      creds = Credentials(datastore=SecretManager,
                          project=config.project, email=config.email)
      return service_builder.build_service(service=self.report_type.service,
                                            key=creds.credentials)

    reader = []
    sa360_service = create_service()

    results = []
    if config.type == ManagerType.FILE_LOCAL:
      action = results.append
      gcs_stored = False
      filename = config.file
      with open(config.file, 'r') as csv_file:
        reader = [row for row in csv.DictReader(csv_file)]

    else:
      query = ActiveAccounts(config)
      # action = query.insert
      gcs_stored = True
      filename = 'custom_columns'
      action = results.append
      job = query.fetch()
      reader = [dict(row) for row in job]
      trunc = query.truncate()

    blocks = self._chunk(reader, 250)
    block_number = 1
    for block in blocks:
      sa360_service = create_service()

      for row in block:
        validator = \
            sa360_validator_factory.SA360ValidatorFactory().get_validator(
                report_type='campaign',
                sa360_service=sa360_service,
                agency=row['ds_agency_id'], advertiser=row['ds_advertiser_id'])
        custom_columns = validator.list_custom_columns()
        result = {
            'agency': row['ds_agency_id'],
            'advertiser': row['ds_advertiser_id'],
            'columns': custom_columns}
        action(result)

      if results:
        self._output_results(results=results, project=config.project,
                             email=config.email, gcs_stored=gcs_stored,
                             file=f'{filename}-{block_number:04d}')
        block_number += 1
        results = []

  def maddie(self, config: ManagerConfiguration, **unused) -> None:
    def _decode_metric(metric: Union[str, SA360ReportMetric]):
      if not metric or metric == '':
        return 'None'

      if isinstance(metric, str):
        return f'{metric}'
      elif isinstance(metric, dict):
        return f"{metric['value']}"
      else:
        return f'{metric.value}'

    sa360_reports = self.firestore.get_all_documents(type=Type.SA360_RPT)
    results = []
    for sa360_report in sa360_reports:
      if sa360_report.id == '_reports':
        continue

      if r := self.firestore.get_document(
              type=Type.SA360_RPT, id=sa360_report.id):
        try:
          report: SA360Job = SA360Job.from_dict(r)

          row = {
              'job name': f'{report.report}_{report.AgencyId}_{report.AdvertiserId}',
              'report name': report.report,
              'agency_id': report.AgencyId,
              'advertiser_id': report.AdvertiserId,
              'dataset': report.dest_dataset,
              'conversion metric': _decode_metric(report.ConversionMetric),
              'revenue metric': _decode_metric(report.RevenueMetric)
          }
          results.append(row)

        except KeyError:
          print(f'Error: {r}')

    if results:
      csv_output = f'{config.email}-maddie.csv'
      if config.gcs_stored:
        csv_bytes = io.StringIO()
        writer = csv.DictWriter(
            csv_bytes, fieldnames=Validation.keys(), quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows([r.to_dict() for r in results])
        Cloud_Storage(project=config.project, email=config.email).write_file(
            bucket=self.bucket,
            file=csv_output,
            data=csv_bytes.getvalue())

      else:
        with open(csv_output, 'w') as csv_file:
          writer = csv.DictWriter(
              csv_file, fieldnames=results[0].keys(), quoting=csv.QUOTE_ALL)
          writer.writeheader()
          writer.writerows(results)

  def validate(self, config: ManagerConfiguration, **unused) -> None:
    sa360_report_definitions = \
        self.firestore.get_document(self.report_type, '_reports')
    validation_results = []

    sa360_objects = self._read_json(config)

    for sa360_object in sa360_objects:
      if sa360_object == '_reports':
        continue
      creds = Credentials(datastore=SecretManager,
                          project=config.project, email=sa360_object['email'])
      sa360_service = \
          service_builder.build_service(service=self.report_type.service,
                                        key=creds.credentials)

      (valid, validation) = \
          self._report_validation(sa360_report_definitions,
                                  sa360_object, sa360_service)
      validation_results.append(validation)

    if validation_results:
      if config.type == ManagerType.BIG_QUERY:
        results = [json.loads(r.to_json()) for r in validation_results]
        # write to BQ
        client = bigquery.Client(project=config.project)
        table = client.dataset(config.dataset).table('sa360_validation')
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)

        client.load_table_from_json(results, table, job_config=job_config)

      else:
        csv_output = f'{config.email}-<now>-validation.csv'
        if config.gcs_stored:
          csv_bytes = io.StringIO()
          writer = csv.DictWriter(
              csv_bytes, fieldnames=Validation.keys(), quoting=csv.QUOTE_ALL)
          writer.writeheader()
          writer.writerows([r.to_dict() for r in validation_results])
          Cloud_Storage(project=config.project, email=config.email).write_file(
              bucket=self.bucket,
              file=csv_output,
              data=csv_bytes.getvalue())

        else:
          with open(csv_output, 'w') as csv_file:
            writer = csv.DictWriter(
                csv_file, fieldnames=Validation.keys(), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows([r.to_dict() for r in validation_results])

  def _report_validation(self,
                         sa360_report_definitions: Dict[str, Any],
                         report: Dict[str, Any],
                         sa360_service: gdiscovery.Resource) -> \
          Tuple[bool, Dict[str, Any]]:
    log.info(
        'Validating %s (%s/%s) on report %s', report.get("agencyName", "-"),
        report["AgencyId"], report["AdvertiserId"], report["report"])

    target_report = sa360_report_definitions[report['report']]
    validator = \
        sa360_validator_factory.SA360ValidatorFactory().get_validator(
            report_type=target_report['report']['reportType'],
            sa360_service=sa360_service,
            agency=report['AgencyId'], advertiser=report['AdvertiserId'])
    report_custom_columns = \
        [column['name'] for column in target_report['parameters']
         if 'is_list' in column]
    valid = True
    validation = Validation(agency=report['AgencyId'],
                            advertiser=report['AdvertiserId'],
                            customColumns=validator.saved_column_names)

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
          log.info(
              f'  Field {report_custom_column} - '
              f'{report[report_custom_column]}: '
              f'{valid_column}, did you mean "{name}"')
        else:
          log.info(
              f'  Field {report_custom_column} - '
              f'{report[report_custom_column]}: '
              f'{valid_column}')

    if len(set(report_custom_columns)) != len(report_custom_columns):
      valid = False

    return (valid, validation)

  def install(self, config: ManagerConfiguration, **unused) -> None:
    if not self.scheduler:
      log.warn(
          'No scheduler is available: jobs will be stored but not scheduled.')

    results = []
    random.seed(uuid.uuid4().bytes)

    runners = self._read_json(config)
    sa360_report_definitions = \
        self.firestore.get_document(self.report_type, '_reports')

    credentials = {}
    services = {}
    for runner in runners:
      id = f"{runner['report']}_{runner['AgencyId']}_{runner['AdvertiserId']}"
      if not runner['dest_dataset']:
        runner['dest_dataset'] = \
            f'sa360_hourly_depleted_{runner["country_code"].lower()}'

      if not (description := runner.get('description')):
        description = (
            f'[{runner["country_code"]}] '
            f'{runner["title"] if "title" in runner else runner["report"]}: '
            f'{runner["agencyName"]}/{runner["advertiserName"]}')
        runner['description'] = description

      if not (creds := credentials.get(runner['email'])):
        creds = Credentials(datastore=SecretManager,
                            project=config.project, email=runner['email'])
        credentials[runner['email']] = creds

      if not (sa360_service := services.get(runner['email'])):
        sa360_service = \
            service_builder.build_service(service=self.report_type.service,
                                          key=creds.credentials)
        services[runner['email']] = sa360_service

      try:
        (valid, validity) = self._report_validation(
            sa360_report_definitions=sa360_report_definitions,
            report=runner, sa360_service=sa360_service)

        if valid:
          log.info('Valid report: %s', id)
          sa360_job = SA360Job.from_dict(runner)
          self.firestore.update_document(type=self.report_type,
                                        id=id, new_data=sa360_job.to_dict())

          if self.scheduler:
            results.append(self._schedule_job(project=config.project,
                                              runner=runner, id=id))

        else:
          log.info('Invalid report: %s', id)
          results.append(f'{id} - Validation failed: {validity}')

      except Exception as e:
        log.info('Validation failed: %s', gmail.error_to_trace(e))
        results.append(f'{id} - Validation failed: {gmail.error_to_trace(e)}')

    if results:
      if config.type == ManagerType.BIG_QUERY:
        query = ManagerUpdate(config)
        job = query.execute()

      else:
        self._output_results(results=results, project=config.project,
                             email=config.email, gcs_stored=config.gcs_stored,
                             file=config.file)
