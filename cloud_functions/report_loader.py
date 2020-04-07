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
import os
import pytz
import traceback

from datetime import datetime
from google.api_core import retry
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJob
from io import BytesIO
from typing import Dict, Any

from classes.firestore import Firestore
from classes.report_type import Type


class ReportLoader(object):
  """Run the report loading process
  
  This performs the CSV import into BQ. It is triggered by a finalize/create on a
  monitored GCS bucket, and will ONLY process CSVs. All other files written to that
  bucket will result in an error in the logs. The file must named the same as the report
  id that is stored in Firestore - this is how the process knows which table/schema to use.

  Once started, the BQ Import Job (of type google.cloud.bigquery.LoadJob) is stored in 
  Firestore, under the 'jobs' key. This is then monitored for completion by JobMonitor.
  """
  CS = storage.Client()     # uses default service account credentials
  FIRESTORE = Firestore()   # uses default service account credentials
  BQ = bigquery.Client()    # uses default service account credentials
  # TODO (davidharcombe@) Make this a parameter or environment variable?
  BQ_DATASET = os.environ.get('BQ_DATASET', 'report2bq')  # default to report2bq dataset for all imports

  def process(self, data: Dict[str, Any], context):
    """Process an added file

    This is the entry point for the Cloud Function to create the BQ import job.
    
    Arguments:
        event {Dict[str, Any]} -- data sent from the PubSub message
        context {Dict[str, Any]} -- context data. unused
    """
    bucket_name = data['bucket']
    file_name = data['name']

    if file_name.upper().endswith('CSV'):
      logging.info('Processing CSV file %s' % file_name)

      try:
        self._handle_csv(bucket_name, file_name)

      except Exception as e:
        logging.error('Error processing file %s\n%s' % (file_name, e))

    else:
      # Ignore it, it's probably the schema
      logging.warn('File added that will not be processed: %s' % file_name)


  def _get_report_config(self, id: int) -> (Type, Dict[str, Any]):
    """Fetch the report configuration

    Load the stored report configuration from Firestore and return the report type
    and config as a tuple
    
    Arguments:
        id {int} -- Report Id, aka CSV file name
    
    Returns:
        (Type, Dict[str, Any]) -- Tuple containing the report type as an Enum, and the
        report configuration.
    """
    config = None
    for config_type in [Type.DBM, Type.DCM, Type.SA360]:
      config = self.FIRESTORE.get_report_config(config_type, id)
      if config: return config_type, config

    return None, None


  def _handle_csv(self, bucket_name: str, file_name: str):
    """Handle the CSV file

    Work out which type of job it is and send it to the appropriate uploader
    
    Arguments:
        bucket_name {str} -- name of the source bucket
        file_name {str} -- name of the CSV file
    """
    logging.warn('Processing: %s' % file_name)

    # Load config file. Must be present to continue
    # This could be either DBM/DV360 or (D)CM
    report_id = file_name.split('/')[-1].split('.')[0]
    config_type, config = self._get_report_config(report_id)

    if not config_type:
      raise Exception('No config found for report %s' % report_id)

    # Insert with schema and table name from config
    if config_type == Type.DBM:
      job = self._import_dbm_report(bucket_name, file_name, config)

    elif config_type == Type.DCM:
      job = self._import_dcm_report(bucket_name, file_name, config)

    elif config_type == Type.SA360:
      job = self._import_sa360_report(bucket_name, file_name, config)

    # Store the completed job in Firestore
    if job:
       self.FIRESTORE.store_import_job_details(report_id, job)


  def _import_dbm_report(self, bucket_name, file_name, config) -> bigquery.LoadJob:
    """Begin DV360 import

    These functions are identical, but need not be (used not to be) to reflect the fact that at
    some point, each product's CSVs could be subtly different, or that on product or another may
    switch from CSV to (say) json.
    
    Arguments:
        bucket_name {str} -- GCS bucket name
        file_name {str} -- CSV file name
        config {Dict[str, Any]} -- report config
    
    Returns:
        bigquery.LoadJob
    """
    return self._import_report(bucket_name, file_name, config)


  def _import_dcm_report(self, bucket_name, file_name, config):
    """Begin CM import

    These functions are identical, but need not be (used not to be) to reflect the fact that at
    some point, each product's CSVs could be subtly different, or that on product or another may
    switch from CSV to (say) json.
    
    Arguments:
        bucket_name {str} -- GCS bucket name
        file_name {str} -- CSV file name
        config {Dict[str, Any]} -- report config
    
    Returns:
        bigquery.LoadJob
    """
    return self._import_report(bucket_name, file_name, config)


  def _import_sa360_report(self, bucket_name, file_name, config):
    """Begin SA360 import

    These functions are identical, but need not be (used not to be) to reflect the fact that at
    some point, each product's CSVs could be subtly different, or that on product or another may
    switch from CSV to (say) json.
    
    Arguments:
        bucket_name {str} -- GCS bucket name
        file_name {str} -- CSV file name
        config {Dict[str, Any]} -- report config
    
    Returns:
        bigquery.LoadJob
    """
    return self._import_report(bucket_name, file_name, config)


  def _import_report(self, bucket_name: str, file_name: str, config: dict) -> bigquery.LoadJob:
    """Begin CSV import

    Create and start the Big Query import job.

    Arguments:
        bucket_name {str} -- GCS bucket name
        file_name {str} -- CSV file name
        config {Dict[str, Any]} -- report config
    
    Returns:
        bigquery.LoadJob
    """
    table_name = config['table_name']
    logging.info("bucket %s, table %s, file_name %s" % (bucket_name, table_name, file_name))

    json_schema = config['schema']
    schema = []
    # Build the json format schema that the BQ LoadJob requires from the text-based ones in the config
    for field in json_schema:
      f = bigquery.schema.SchemaField(name=field['name'],
                                      field_type=field['type'],
                                      mode=field['mode'])
      schema.append(f)

    table_ref = self.BQ.dataset(self.BQ_DATASET).table(table_name)

    # Default action is to completely replace the table each time. If requested, however then
    # we can do an append for (say) huge jobs where you would see the table with 60 days once
    # and then append 'yesterday' each day.
    import_type = bigquery.WriteDisposition.WRITE_TRUNCATE if not config.get('append', False) else bigquery.WriteDisposition.WRITE_APPEND

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = import_type
    # Assume a CSV header is the first line unless otherwise specified in the report's own config
    job_config.skip_leading_rows = config.get('csv_header_length', 1)
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema = schema
    # Allow a few errors, just in case
    job_config.max_bad_records = 10
    # Allow for DV360/CM (SA360 won't) to pass jagged rows, which they do
    job_config.allow_jagged_rows = True
    
    uri = "gs://%s/%s" % (bucket_name, file_name)
    load_job = self.BQ.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    logging.info("Starting CSV import job {}".format(load_job.job_id))

    return load_job
                  