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
import re

from classes import csv_helpers
from classes import decorators
from classes import ReportFetcher
from classes import fetcher_factory
from classes import csv_helpers
from classes.sa360_dynamic import SA360Dynamic
from classes.sa360_web import SA360Web
from classes.firestore import Firestore
from classes.report_type import Type

from typing import Any, Dict, List
from urllib.parse import unquote


class Report2BQ(object):
  def __init__(self, product: Type, email: str=None, project: str=None,
    report_id: str=None, profile: str=None, sa360_url: str=None,
    force: bool=False, append: bool=False, infer_schema: bool=False,
    dest_project: str=None, dest_dataset: str='report2bq', dest_table: str=None,
    notify_message: str=None, file_id: str=None, partition: str=None,
    **unused) -> Report2BQ:
    self.product = product

    self.force = force
    self.email = email
    self.append = append
    self.infer_schema = infer_schema
    self.in_cloud = unused.get('in_cloud', True)

    self.report_id = report_id

    self.sa360_url = unquote(sa360_url) if sa360_url else None
    self.file_id = file_id

    self.cm_profile = profile

    self.project = project

    self.dest_project = dest_project
    self.dest_dataset = dest_dataset
    self.dest_table = dest_table

    self.notify_message = notify_message
    self.partition = partition

  @decorators.lazy_property
  def firestore(self) -> Firestore:
    return Firestore() #email=self.email, project=self.project)

  def handle_report_fetcher(self, fetcher: ReportFetcher) -> None:
    def _schema(field):
      if self.partition == 'infer' and \
        field['type'] not in ['DATE', 'DATETIME']:
        field['type'] = 'STRING'
      return field

    # Get Latest Report
    report_object = fetcher.get_latest_report_file(self.report_id)

    # Normalize Report Details
    report_data = \
      fetcher.fetch_report_config(
        report_object=report_object, report_id=self.report_id)
    last_report = \
      self.firestore.get_document(fetcher.report_type, self.report_id)

    if last_report:
      if report_data['last_updated'] == \
        last_report['last_updated'] and not self.force:
        logging.info('No change: ignoring.')
        return

    report_data = \
      fetcher.normalize_report_details(
        report_object=report_object, report_id=self.report_id)

    report_data['email'] = self.email
    report_data['append'] = self.append

    if self.dest_project: report_data['dest_project'] = self.dest_project
    if self.dest_dataset: report_data['dest_dataset'] = self.dest_dataset

    if self.dest_table:
      table_name = (csv_helpers.sanitize_string(self.dest_table))
    else:
      table_name = report_data.get("report_name", "unnamed_report")
    report_data['dest_table'] = \
      f'{fetcher.report_type}_{self.report_id}_{table_name}'

    if self.notify_message:
      report_data['notifier'] = {
        'message': self.notify_message,
      }

    if report_object:
      csv_header, csv_types = fetcher.read_header(report_data)
      if csv_header:
        self._handle_partitioning(
          report_data=report_data, csv_header=csv_header, csv_types=csv_types)

        fetcher.stream_to_gcs(f'{self.project}-report2bq-upload', report_data)

    self.firestore.store_document(type=fetcher.report_type, id=self.report_id,
                                  document=report_data)

  def handle_sa360(self):
    sa360 = SA360Web(
      project=self.project,
      email=self.email,
      infer_schema=self.infer_schema,
      append=self.append)
    logging.info(self.sa360_url)
    id = re.match(r'^.*rid=([0-9]+).*$', self.sa360_url).group(1)
    report_data = self.firestore.get_document(Type.SA360, id)

    if not report_data:
      report_data = {
        'id': id,
        'url': self.sa360_url,
        'email': self.email,
      }

    if self.dest_project: report_data['dest_project'] = self.dest_project
    if self.dest_dataset: report_data['dest_dataset'] = self.dest_dataset
    if self.dest_table:
      table_suffix = f'{"_" + csv_helpers.sanitize_string(self.dest_table)}'
    else:
      table_suffix = ''
    report_data['dest_table'] = f'{Type.SA360}_{id}{table_suffix}'

    if self.notify_message:
      report_data['notifier'] = {
        'message': self.notify_message,
      }

    csv_header, csv_types = sa360.stream_to_gcs(
      bucket=f'{self.project}-report2bq-upload',
      report_details=report_data)

    self._handle_partitioning(
      report_data=report_data, csv_header=csv_header, csv_types=csv_types)

    self.firestore.store_document(type=Type.SA360, id=id, document=report_data)

  def handle_sa360_report(self):
    sa360 = SA360Dynamic(
      project=self.project,
      email=self.email,
      infer_schema=self.infer_schema,
      append=self.append)
    logging.info(f'Handling SA360 report {self.report_id}')

    # Merge configs
    run_config = {
      "email": self.email,
      "file_id": self.file_id,
      "project": self.project,
      "report_id": self.report_id,
      "type": self.product,
    }
    if sa360.handle_report(run_config=run_config):
      self.firestore.remove_report_runner(self.report_id)
      logging.info(f'Report {self.report_id} done.')

    else:
      # SA360 ones can't fail - they won't start if there are errors, so it's
      # just not ready yet. So just leave it here and try again later.
      logging.error(f'Report {self.report_id} not ready.')

  def _handle_partitioning(
    self, report_data: Dict[str, Any], csv_header: List[str],
    csv_types: List[str]) -> None:
    def _field_fix(field: Dict[str, str]) -> Dict[str, str]:
      if self.partition == 'infer' and field['type'] in ['DATE', 'DATETIME']:
        return field
      elif not self.infer_schema:
        field['type'] = 'STRING'
      return field

    schema = list(
      map(_field_fix, csv_helpers.create_table_schema(csv_header, csv_types)))
    report_data['schema'] = schema
    if self.partition == 'infer':
      msg = [ f'{F["name"]} - {F["type"]}' for F in schema ]
      date_columns = \
        [F['name'] for F in schema if F['type'] in ['DATE', 'DATETIME']]
      if date_columns:
        report_data['partition'] = self.partition
        report_data['partition_column'] = date_columns[0]
      else:
        logging.info(
          'Inferred partitioning requested, but no DATE[TIME] columns '
          'found in schema: %s', ", ".join(msg))
        if 'partition' in report_data:
          report_data.pop('partition')
    elif self.partition:
      report_data['partition'] = self.partition

  def run(self):
    logging.info(f'Product: {self.product}')
    if self.product in [ Type.DV360, Type.CM ]:
      fetcher = fetcher_factory.create_fetcher(self.product,
                                               email=self.email,
                                               project=self.project,
                                               profile=self.cm_profile)
      self.handle_report_fetcher(fetcher=fetcher)

    elif self.product == Type.SA360:
      self.handle_sa360()

    elif self.product == Type.SA360_RPT:
      self.handle_sa360_report()

    else:
      raise NotImplementedError('Unknown report type requested')