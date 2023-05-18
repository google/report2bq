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
from __future__ import annotations

import json
import os
from io import BytesIO, StringIO
from typing import Any, Dict

from auth.credentials import Credentials
from auth.datastore.secret_manager import SecretManager
from service_framework import service_builder

from classes import (ReportRunner, csv_helpers, decorators, ga360_report,
                     ga360_report_response)
from classes.gcs_streaming import GCSObjectStreamUpload
from classes.report_type import Type


class GA360ReportRunner(ReportRunner):
  """GA360ReportRunner.

  On demand runner for GA360 Reporting API.
  """
  report_type = Type.GA360_RPT

  def __init__(self, report_id: str, email: str, project: str = None,
               **kwargs) -> GA360ReportRunner:
    """Initialize the runner.

    The runner inherits from ReportRunner, which mandates the 'run' method.

    Args:
        report_id (str, optional): GA360 report id. Defaults to None.
        email (str, optional): User email for the token. Defaults to None.
        project (str, optional): Project. Defaults to None but should be
          pre-populated with the current project by the caller.
        kwargs: All the other keyword arguments, if any.

    Returns:
        GA360ReportRunner: self
    """
    self._email = email
    self._project = project
    self._report_id = report_id
    self._kwargs = kwargs

  @decorators.lazy_property
  def credentials(self) -> Credentials:
    return Credentials(datastore=SecretManager, project=self._project,
                       email=self._email)

  def run(self, unattended: bool = False) -> Dict[str, Any]:
    """Perform the report run

    Args:
        unattended (bool, optional): Is this a fire and forget (True) or wait
          for the report to complete (False). Defaults to True.
    """
    if unattended:
      return self._unattended_run()
    else:
      return self._attended_run()

  def _attended_run(self) -> None:
    """Performs a GA360 report, waiting for the run to finish.

    Run the GA360 Report and store the resultant schema in Firstore. Then send
    the CSV to GCS for processing by the report loader.
    """
    runner = None
    report_config = None
    try:
      runner = self.firestore.get_document(
          type=self.report_type, id=self._report_id)

      if report_config := self.firestore.get_document(type=self.report_type,
                                                      id='_reports',
                                                      key=runner.get('report')):
        definition = ga360_report.GA360ReportDefinition.from_json(
            json.dumps(report_config))
      else:
        raise NotImplementedError(f'No such runner: {self._report_id}')

      definition.view_id = runner.get('view_id')
      ranges = []
      for date_range in runner.get('date_ranges'):
        range = \
            ga360_report.GA360DateRange(start_date=date_range.get('start_date'),
                                        end_date=date_range.get('end_date'))
        ranges.append(range)
      definition.date_ranges = ranges

      request_body = {
          'reportRequests': [
              definition.report_request
          ]
      }
      ga360_service = service_builder.build_service(
          service=self.report_type.service,
          key=self.credentials.credentials)
      request = ga360_service.reports().batchGet(body=request_body)
      response = request.execute()

      # TODO: Right now, process just the first result, we're not allowing for
      #       multiple reports in the same request.
      #       Also, we are assuming that this whole report can be processed in
      #       a single chunk in memory. If this is not the case, we have a much,
      #       much larger issue to handle a json file in pieces. This is not
      #       supported currently.
      #         -- davidharcombe@, 2021/04/09
      if report := response.get('reports'):
        report_json = json.dumps(report[0])
        result = \
            ga360_report_response.GA360ReportResponse.from_json(report_json)

        # Convert report into CSV - handled by the dataclass itself.
        output_buffer = StringIO()
        result.to_csv(output=output_buffer)

        # Write schema to Firestore - update like any other.
        headers, types = csv_helpers.get_column_types(
            BytesIO(output_buffer.getvalue().encode('utf-8')))
        schema = \
            csv_helpers.create_table_schema(column_headers=headers,
                                            column_types=None)
        runner['schema'] = schema
        self.firestore.update_document(self.report_type, self._report_id,
                                       runner)

        # Stream CSV to GCS. Should beable to use un-threaded streamer.
        # We look for a 'CHUNK_MULTIPLIER' setting in the environment, like
        # everywhere else, but default to 128, making the standard chunk
        # size we process 128Mb. Well within the 4Gb we're allowed for a
        # cloud function. If they turn out to be bigger than this (which I
        # # don't believe GA360 reports will be), we should move to the
        # ThreadedGCSObjectStreamUpload version.
        chunk_size = os.environ.get('CHUNK_MULTIPLIER', 128) * 1024 * 1024
        streamer = GCSObjectStreamUpload(
            creds=self.credentials.credentials,
            bucket_name=f'{self._project}-report2bq-upload',
            blob_name=f'{self._report_id}.csv',
            chunk_size=chunk_size)
        streamer.begin()

        output_buffer.seek(0)
        with output_buffer as source:
          chunk = source.read(chunk_size).encode('utf-8')
          streamer.write(chunk)

        streamer.stop()
        # Profit!!!

    except Exception as e:
      self._email_error(email=self._email, error=e, report_config=report_config,
                        message=f'Error in GA360 Report Runner for report {self._report_id}\n\n')

    finally:
      return runner

  def _unattended_run(self) -> Dict[str, Any]:
    """_unattended_run.

    Not implemented for GA360.

    Raises:
        NotImplementedError
    """
    raise NotImplementedError('Unavailable for GA360 reports.')
