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

import logging
import os
from typing import Any, Dict, List, Tuple

from auth.credentials import Credentials as Report2BQCredentials
from auth.secret_manager import SecretManager
from classes import csv_helpers, decorators, firestore, gmail
from classes.report_type import Type
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2.credentials import Credentials


class ReportLoader(object):
  """Run the report loading process

  This performs the CSV import into BQ. It is triggered by a finalize/create
  on a monitored GCS bucket, and will ONLY process CSVs. All other files
  written to that bucket will result in an error in the logs. The file must be
  named the same as the report id that is stored in Firestore - this is how the
  process knows which table/schema to use.

  Once started, the BQ Import Job (of type google.cloud.bigquery.LoadJob) is
  stored in Firestore, under the 'jobs' key. This is then monitored for
  completion by JobMonitor.
  """

  @decorators.lazy_property
  def firestore(self) -> firestore.Firestore:
    return firestore.Firestore()

  def process(self, data: Dict[str, Any], context):
    """Process an added file

    This is the entry point for the Cloud Function to create the BQ import job.

    Arguments:
        event (Dict[str, Any]):  data sent from the PubSub message
        context (Dict[str, Any]):  context data. unused
    """
    bucket_name = data['bucket']
    file_name = data['name']

    if file_name.upper().endswith('CSV'):
      logging.info('Processing CSV file %s', file_name)

      try:
        self._handle_csv(bucket_name, file_name)

      except Exception as e:
        logging.error('Error processing file %s\n%s', file_name,
                      gmail.error_to_trace(e))

  def _get_report_config(self, id: str) -> Tuple[Type, Dict[str, Any]]:
    """Fetch the report configuration

    Load the stored report configuration from Firestore and return the report
    type and config as a tuple

    Arguments:
        id (int):  Report Id, aka CSV file name

    Returns:
        (Type, Dict[str, Any]): Tuple containing the report type as an Enum,
                                  and the report configuration.
    """
    for config_type in [
        Type.DV360, Type.CM, Type.SA360, Type.SA360_RPT, Type.GA360_RPT,
    ]:
      if config := self.firestore.get_document(config_type, id):
        return config_type, config

    return None, None

  def _handle_csv(self, bucket_name: str, file_name: str):
    """Handle the CSV file

    Work out which type of job it is and send it to the appropriate uploader

    Arguments:
        bucket_name (str):  name of the source bucket
        file_name (str):  name of the CSV file
    """
    # Load config file. Must be present to continue
    report_id = file_name.split('/')[-1].split('.')[0]
    config_type, config = self._get_report_config(report_id)

    if not config:
      self._email_error(f'No config found for report {report_id}')
      raise Exception(f'No config found for report {report_id}')

    # Store the completed job in Firestore
    if job := self._import_report(bucket=bucket_name,
                                  file=file_name,
                                  config_type=config_type,
                                  config=config):
      self.firestore.store_document(type=Type._JOBS, id=report_id,
                                    document=job.to_api_repr())

  def _import_report(self,
                     bucket: str,
                     file: str,
                     config_type: Type,
                     config: dict) -> bigquery.LoadJob:
    """Begin CSV import

    Create and start the Big Query import job.

    Arguments:
        bucket (str):  GCS bucket name
        file (str):  CSV file name
        config)_type (Type):  report type
        config (Dict[str, Any]):  report config

    Returns:
        bigquery.LoadJob
    """
    bq = self._find_client(config)

    # Use 'dest_dataset' if it is set specifically for this table. If not,
    # then use the project environment version (again, if set) and failing all
    # that, fall back to 'report2bq'.
    dataset = \
        config.get('dest_dataset') \
        or os.environ.get('BQ_DATASET') \
        or 'report2bq'

    # Split up the file and find the base name, without extension. If the
    # uri is 'gs://my-project-report2bq-upload/my_file.csv', then the file name
    # is 'my_file'.
    base_file = file.split('/')[-1].split('.')[0]

    # To avoid breaking existing configs, use 'table_name' if it is present.
    # Net new configs will not have a 'table_name' key, instead having
    # dest_table' so use that instead. Failing that, just use the file name.
    table_name = \
        config.get('table_name') or \
        config.get('dest_table') or \
        csv_helpers.sanitize_title(base_file)

    logging.info('bucket %s, table %s, file_name %s',
                 bucket, table_name, file)

    # Build the json format schema that the BQ LoadJob requires
    json_schema = config['schema']
    schema = []
    _json_schema = []
    for field in json_schema:
      f = bigquery.schema.SchemaField(name=field['name'],
                                      field_type=field['type'],
                                      mode=field['mode'])
      schema.append(f)
      _json_schema.append(f'{field["name"]}: {field["type"]}')

    table_ref = bq.dataset(dataset).table(table_name)
    import_type = bigquery.WriteDisposition.WRITE_TRUNCATE

    if self._table_exists(bq, table_ref):
      # Check for "forced update"
      if config.pop('drop_table', False):
        try:
          bq.delete_table(table_ref)
        finally:
          self.firestore.update_document(type=config_type, id=config['id'],
                                         new_data=config)

      # Default action is to completely replace the table each time. If
      # requested, however then we can do an append for (say) huge jobs where
      # you would seed the table with 60 days once and then append
      # yesterday's results each day.
      if config.get('append', False):
        self._validate_schema(bq, table_ref, schema)
        import_type = bigquery.WriteDisposition.WRITE_APPEND

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = import_type
    # Assume a CSV header is the first line.
    job_config.skip_leading_rows = config.get('csv_header_length', 1)
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema = schema
    # Allow a few errors, just in case. You'll need it.
    job_config.max_bad_records = 10
    # Allow for DV360/CM (SA360 won't) to pass jagged rows, which they do.
    job_config.allow_jagged_rows = True

    # Partitioning
    if config.get('partition'):
      job_config.time_partitioning = \
          bigquery.TimePartitioning(
              type_=bigquery.TimePartitioningType.DAY,
              field=config.get('partition_column'))

    uri = f'gs://{bucket}/{file}'
    load_job = bq.load_table_from_uri(uri, table_ref, job_config=job_config)
    logging.info('Starting CSV import job %s', load_job.job_id)

    return load_job

  def _find_client(self, config: Dict[str, Any]) -> bigquery.Client:
    if config.get('dest_project'):
      # authenticate against supplied project with supplied key
      project = config.get('dest_project')
      r2bq = Report2BQCredentials(datastore=SecretManager,
                                  project=os.environ.get('GCP_PROJECT'),
                                  email=config['email'])
      client_key = r2bq.token_details
      client_key['client_id'] = \
          (r2bq.project_credentials.get('web') or
           r2bq.project_credentials.get('installed')).get('client_id')
      client_key['client_secret'] = \
          (r2bq.project_credentials.get('web') or
           r2bq.project_credentials.get('installed')).get('client_secret')
      creds = Credentials.from_authorized_user_info(client_key)
      bq = bigquery.Client(project=project, credentials=creds)

    else:
      bq = bigquery.Client()

    return bq

  def _schema_to_string(self, schema: List[bigquery.SchemaField]) -> str:
    _schema = [f'{field.name}, {field.field_type}' for field in schema]
    return '\n'.join(_schema)

  def _table_exists(self,
                    bq: bigquery.Client,
                    table_ref: bigquery.TableReference) -> bool:
    try:
      bq.get_table(table_ref)
      return True

    except NotFound:
      return False

  def _validate_schema(self,
                       bq: bigquery.Client,
                       table_ref: bigquery.TableReference,
                       schema: List[bigquery.schema.SchemaField],
                       config: Dict[str, Any]) -> bool:
    _table = bq.get_table(table_ref)
    _schema = _table.schema

    if _valid := (schema == _schema):
      self._email_error(
          email=config['email'],
          message=f'''
Mismatched schema for {_table.full_table_id}, trying anyway

Report has schema:
{self._schema_to_string(schema)}

Table has schema:
{self._schema_to_string(bq.get_table(table_ref).schema)}
'''
      )
      logging.error('Mismatched schema for %s, trying anyway',
                    _table.full_table_id)

    return _valid

  def _email_error(self,
                   message: str,
                   email: str = None,
                   error: Exception = None) -> None:
    to = [email] if email else []
    administrator = \
        self.firestore.get_document(Type._ADMIN, 'admin').get('email') or \
        os.environ.get('ADMINISTRATOR_EMAIL')
    cc = [administrator] if administrator else []
    body = f'{message}{gmail.error_to_trace(error)}'

    if to or cc:
      message = gmail.GMailMessage(
          to=to,
          cc=cc,
          subject=f'Error in report_loader',
          body=body,
          project=os.environ.get('GCP_PROJECT'))

      gmail.send_message(
          message=message,
          credentials=Report2BQCredentials(
              datastore=SecretManager,
              email=email, project=os.environ.get('GCP_PROJECT'))
      )
