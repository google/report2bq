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
from google.cloud.exceptions import NotFound
from google.oauth2.credentials import Credentials

from io import BytesIO
from typing import Any, Dict, List, Tuple

from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials as Report2BQCredentials
from classes.csv_helpers import CSVHelpers
from classes.firestore import Firestore
from classes.gmail import GMail, GMailMessage
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


  def process(self, data: Dict[str, Any], context):
    """Process an added file

    This is the entry point for the Cloud Function to create the BQ import job.
    
    Arguments:
        event {Dict[str, Any]} -- data sent from the PubSub message
        context {Dict[str, Any]} -- context data. unused
    """
    logging.info(data)
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


  def _get_report_config(self, id: str) -> (Type, Dict[str, Any]):
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
    for config_type in [Type.DV360, Type.CM, Type.SA360, Type.SA360_RPT]:
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
    # Load config file. Must be present to continue
    # This could be either DBM/DV360 or (D)CM
    report_id = file_name.split('/')[-1].split('.')[0]
    config_type, config = self._get_report_config(report_id)

    if not config_type:
      self._email_error(f'No config found for report {report_id}')
      raise Exception(f'No config found for report {report_id}')

    logging.info(config)

    # Insert with schema and table name from config
    if config_type == Type.DV360:
      job = self._import_dbm_report(bucket_name, file_name, config)

    elif config_type == Type.CM:
      job = self._import_dcm_report(bucket_name, file_name, config)

    elif config_type == Type.SA360:
      job = self._import_sa360_report(bucket_name, file_name, config)

    elif config_type == Type.SA360_RPT:
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
    if config.get('dest_project'):
      # authenticate against supplied project with supplied key
      project = config.get('dest_project') or os.environ.get('GCP_PROJECT')
      client_key = json.loads(Cloud_Storage.fetch_file(
        bucket=f"{os.environ.get('GCP_PROJECT')}-report2bq-tokens",
        file=f"{config['email']}_user_token.json"
      ))
      server_key = json.loads(Cloud_Storage.fetch_file(
        bucket=f"{os.environ.get('GCP_PROJECT')}-report2bq-tokens",
        file='client_secrets.json'
      ))
      client_key['client_id'] = (server_key.get('web') or server_key.get('installed')).get('client_id')
      client_key['client_secret'] = (server_key.get('web') or server_key.get('installed')).get('client_secret')
      logging.info(client_key)
      creds = Credentials.from_authorized_user_info(client_key)
      bq = bigquery.Client(project=project, credentials=creds)

    else:
      project = os.environ.get('GCP_PROJECT')
      bq = bigquery.Client()

    dataset = config.get('dest_dataset') or os.environ.get('BQ_DATASET') or 'report2bq'

    table_name = config.get('table_name', CSVHelpers.sanitize_string(file_name))
    logging.info(f'bucket {bucket_name}, table {table_name}, file_name {file_name}')

    json_schema = config['schema']
    schema = []
    _json_schema = []
    # Build the json format schema that the BQ LoadJob requires from the text-based ones in the config
    for field in json_schema:
      f = bigquery.schema.SchemaField(name=field['name'],
                                      field_type=field['type'],
                                      mode=field['mode'])
      schema.append(f)
      _json_schema.append(f'{field["name"]}: {field["type"]}')

    table_ref = bq.dataset(dataset).table(table_name)

    # Default action is to completely replace the table each time. If requested, however then
    # we can do an append for (say) huge jobs where you would see the table with 60 days once
    # and then append 'yesterday' each day.
    if config.get('append', False):
      if self._table_exists(bq, table_ref) and not self._validate_schema(bq, table_ref, schema):
        config_schema = '\n'.join([ f'{field.name}, {field.field_type}' for field in schema])
        target_schema = '\n'.join([ f'{field.name}, {field.field_type}' for field in bq.get_table(table_ref).schema])
        self._email_error(
          email=config['email'], 
          message=f'''
Mismatched schema for {project}.{dataset}.{table_name}, trying anyway

Report has schema:
{config_schema}

Table has schema:
{target_schema}
'''
        )
        logging.error(f"Mismatched schema for {project}.{dataset}.{table_name}, trying anyway")

      import_type = bigquery.WriteDisposition.WRITE_APPEND
      
    else:
      import_type = bigquery.WriteDisposition.WRITE_TRUNCATE

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
    
    uri = f'gs://{bucket_name}/{file_name}'
    load_job = bq.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    logging.info(f'Starting CSV import job {load_job.job_id}')

    return load_job


  def _table_exists(self, bq: bigquery.Client, table_ref: bigquery.TableReference) -> bool:
    try:
        bq.get_table(table_ref)
        return True

    except NotFound:
        return False


  def _validate_schema(self, bq: bigquery.Client, table_ref: bigquery.TableReference, schema: List[bigquery.schema.SchemaField]) -> bool:
    _table = bq.get_table(table_ref)
    _schema = _table.schema

    return _schema == schema


  def _email_error(self, message: str, email: str=None, error: Exception=None) -> None:
    _to = [email] if email else []
    _administrator = os.environ.get('ADMINISTRATOR_EMAIL') or self.FIRESTORE.get_document(Type._ADMIN, 'admin').get('email')
    _cc = [_administrator] if _administrator else []

    if _to or _cc:
      message = GMailMessage(
        to=_to, 
        cc=_cc,
        subject=f'Error in report_loader',
        body=f'''
{message}

Error: {error if error else 'No exception.'}
''', 
        project=os.environ.get('GCP_PROJECT'))

      GMail().send_message(
        message=message,
        credentials=Report2BQCredentials(email=email, project=os.environ.get('GCP_PROJECT'))
      )
