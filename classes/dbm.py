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

import datetime
import io
import logging


from classes import Fetcher, ReportFetcher
from classes import credentials
from classes import csv_helpers
from classes import decorators
from classes.cloud_storage import Cloud_Storage
from classes.gcs_streaming import ThreadedGCSObjectStreamUpload
from classes.report_type import Type
from classes.services import Service
from messytables.types import CellType

from contextlib import closing
from queue import Queue
from typing import Dict, Any, List, Tuple
from urllib.request import urlopen


class DBM(ReportFetcher, Fetcher):
  report_type = Type.DV360
  service_definition = Service.DV360

  def __init__(self, email: str, project: str, profile: str=None) -> DBM:
    self.email = email
    self.project = project

  def get_reports(self) -> List[Dict[str, Any]]:
    """Fetches the list of reports.

    Returns:
      result (Dict): the list of reports for the current user.
    """
    result = self.fetch(
      self.service.queries().listqueries,
      **{ 'fields':
            ('queries(metadata(googleCloudStoragePathForLatestReport,'
             'latestReportRunTimeMs,title),params/type,queryId,'
             'schedule/frequency)')}
    )

    return result

  def get_latest_report_file(self, report_id: str) -> Dict[str, Any]:
    """Fetch the last known successful report's definition.

    Args:
      report_id: report id
    Returns:
      result (Dict): the last known report, or an empty Dict if it has
                     not yet run.
    """
    report = {}
    if all_results := self.fetch(self.service.reports().listreports,
                                 **{ 'queryId': report_id }):
      if 'reports' in all_results:
        # filter out any still running reports or ones with no 'finishTimeMs'
        results = \
          filter(lambda item: 'finishTimeMs' in \
                              item.get('metadata', {}).get('status', {}),
                              all_results['reports'])
        ordered = \
          sorted(results,
                 key=lambda k: int(k['metadata']['status']['finishTimeMs']))
        report = ordered[-1]
      else:
        logging.info('No reports - has this report run successfully yet?')

    return report

  @decorators.retry(Exception, tries=3, delay=15, backoff=2)
  def normalize_report_details(self,
                               report_object: Dict[str, Any],
                               report_id: str) -> Dict[str, Any]:
    """Normalizes the api format report into a flattened data structure.

    Args:
      report_object: Report details from api queries method
      report_id: the report id.

    Returns:
      result (Dict): the normalized data structure
    """
    query_object = \
      self.service.queries().getquery(queryId=report_id).execute()

    # Check if report has ever completed a run
    try:
      gcs_path = \
        query_object['metadata']['googleCloudStoragePathForLatestReport']
      latest_runtime = query_object['metadata']['latestReportRunTimeMs']
    except KeyError:
      gcs_path = ''
      latest_runtime = 0

    report_data = {
      'id': query_object['queryId'],
      'name': query_object['metadata']['title'],
      'report_name':
          csv_helpers.sanitize_string(query_object['metadata']['title']),
      'type': query_object['params']['type'],
      'current_path': gcs_path,
      'last_updated':
          datetime.datetime.fromtimestamp(
            float(latest_runtime) / 1000.0).strftime("%Y%m%d%H%M"),
      'update_cadence': query_object.get('schedule', {}).get('frequency'),
    }

    return report_data

  @decorators.retry(Exception, tries=3, delay=15, backoff=2)
  def run_report(self, report_id: int,
                 asynchronous: bool=True) -> Dict[str, Any]:
    """Runs a report on the product.

    Args:
        report_id (int): the report to run.
        asynchronous (bool): fire and forget or wait for the result.

    Returns:
        Dict[str, Any]: the run result
    """
    result = {}
    request = self.service.queries().runquery(queryId=report_id,
                                              asynchronous=asynchronous)
    result = request.execute()

    return result

  @decorators.retry(Exception, tries=3, delay=15, backoff=2)
  def report_state(self, report_id: int):
    request = self.service.reports().listreports(queryId=report_id)
    results = request.execute()

    if results:
      ordered = \
        sorted(results['reports'],
               key=lambda k: int(k['metadata']['reportDataStartTimeMs']))
      return ordered[-1]['metadata']['status']['state']

    else:
      return 'UNKNOWN'

  def read_header(self,
                  report_details: dict) -> Tuple[List[str], List[CellType]]:
    """Reads the header of the report CSV file.

    Args:
        report_details (dict): the report definition

    Returns:
        Tuple[List[str], List[CellType]]: the csv headers and column types
    """
    if path := report_details.get('current_path'):
      with closing(urlopen(path)) as report:
        data = report.read(self.chunk_multiplier * 1024 * 1024)
        bytes_io = io.BytesIO(data)
      return csv_helpers.get_column_types(bytes_io)

    else:
      return (None, None)

  @decorators.measure_memory
  def stream_to_gcs(self, bucket: str, report_details: Dict[str, Any]) -> None:
    """Streams the report CSV to Cloud Storage.

    Arguments:
        bucket {str} -- GCS Bucket
        report_details {dict} -- Report definition
    """
    if not 'current_path' in report_details:
      return

    queue = Queue()

    report_id = report_details['id']
    chunk_size = self.chunk_multiplier * 1024 * 1024
    out_file = io.BytesIO()

    streamer = \
      ThreadedGCSObjectStreamUpload(
        client=Cloud_Storage.client(),
        creds=credentials.Credentials(
          email=self.email, project=self.project).get_credentials(),
        bucket_name=bucket,
        blob_name=f'{report_id}.csv',
        chunk_size=chunk_size,
        streamer_queue=queue)
    streamer.start()

    with closing(urlopen(report_details['current_path'])) as _report:
      _downloaded = 0
      chunk_id = 1
      _report_size = int(_report.headers['content-length'])
      logging.info('Report is %s bytes', f'{_report_size:,}')
      while _downloaded < _report_size:
        chunk = _report.read(chunk_size)
        _downloaded += len(chunk)
        if _downloaded >= _report_size:
          # last chunk... trim to footer if there is one, or last blank line if not
          # NOTE: if no blank line (partial file?) NO TRIMMING WILL HAPPEN
          # THIS SHOULD NEVER BE THE CASE
          last = io.BytesIO(chunk)

          # find the footer
          blank_line_pos = chunk.rfind(b'\n\n')

          # if we don't find it, there's no footer.
          if blank_line_pos == -1:
            logging.error('No footer delimiter found. Writing entire final chunk as is.')
            queue.put(chunk)

          else:
            # read the footer
            last.seek(blank_line_pos)
            footer = last.readlines()
            group_count = sum(g.startswith(b'Group By:') for g in footer)
            total_block_start = chunk.rfind(b'\n' + b',' * group_count)

            if total_block_start == -1:
              last.truncate(blank_line_pos)

            else:
              last.truncate(total_block_start)

            queue.put(last.getvalue())
            # break

        else:
          queue.put(chunk)

        chunk_id += 1

    queue.join()
    streamer.stop()
