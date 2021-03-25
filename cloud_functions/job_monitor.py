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

from contextlib import suppress
import json
import logging
import os
import re

from typing import Any, Dict
from classes.cloud_storage import Cloud_Storage
from classes.firestore import Firestore
from classes.report_type import Type

from google.oauth2.credentials import Credentials
from google.cloud import bigquery
from google.cloud import pubsub
from google.cloud import storage
from google.cloud.bigquery import LoadJob


class JobMonitor(object):
  """The process watching running Big Query import jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch
  a queue (held in Firestore) for running BigQuery csv import jobs started as a
  result of the Report2BQ process. Upon successful completion, it deletes the
  file from Cloud Storage. If this job is not run, the last file will remain
  in GCS.
  """


  def process(self, data: Dict[str, Any], context):
    """Check all the running jobs

    Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
    """
    firestore = Firestore()
    documents = firestore.get_all_jobs()

    for document in documents:
      if (job_type := Type(document.get('type'))) != Type._UNKNOWN:
        if config := firestore.get_report_config(job_type, document.id):
          if config.get('dest_project'):
            # authenticate against supplied project with supplied key
            project = \
              config.get('dest_project') or os.environ.get('GCP_PROJECT')
            client_key = json.loads(Cloud_Storage.fetch_file(
              bucket=f"{project}-report2bq-tokens",
              file=f"{config['email']}_user_token.json"
            ))
            server_key = json.loads(Cloud_Storage.fetch_file(
              bucket=f"{project}-report2bq-tokens",
              file='client_secrets.json'
            ))
            client_key['client_id'] = \
              (server_key.get('web') or \
                server_key.get('installed')).get('client_id')
            client_key['client_secret'] = \
              (server_key.get('web') or \
                server_key.get('installed')).get('client_secret')
            logging.info(client_key)
            creds = Credentials.from_authorized_user_info(client_key)
            bq = bigquery.Client(project=project, credentials=creds)

          else:
            bq = bigquery.Client()

          api_repr = document.get().to_dict()
          if api_repr:
            try:
              job = LoadJob.from_api_repr(api_repr, bq)
              job.reload()

              if job.state == 'DONE':
                if job.error_result:
                  logging.error(job.errors)

                self._handle_finished(job=job, config=config)
                ('notifier' in config) and self.notify(
                  report_type=job_type, config=config, job=job, id=document.id)
                firestore.mark_import_job_complete(document.id, job,)

            except Exception as e:
              logging.error(
                'Error loading job %s for monitoring.', document.id)

          break

  def _handle_finished(self, job: LoadJob, config: Dict[str, Any]):
    """Deal with completed jobs

    When we find a completed job, delete the source CSV from GCS.

    Arguments:
        job {LoadJob} -- Big Query import job
    """
    for source in job.source_uris:
      match = re.match(r'gs://([^/]+)/(.*)', source)

      bucket_name = match[1]
      blob_name = match[2]

      source_bucket = storage.Client().get_bucket(bucket_name)
      source_blob = source_bucket.blob(blob_name)
      with suppress(Exception):
        if config.get('development'):
          return

        source_blob.delete()
        logging.info('File %s removed from %s.', blob_name, bucket_name)

  def notify(self,
             report_type: Type,
             config: Dict[str, Any],
             job: LoadJob, id: str):
    columns = ';'.join([ field['name'] for field in config['schema'] ])

    attributes = {
      'project': job.destination.project,
      'dataset': job.destination.dataset_id,
      'table': job.destination.table_id,
      'rows': str(job.output_rows),
      'id': id,
      'type': report_type.value,
      'columns': columns
    }

    client = pubsub.PublisherClient()
    try:
      project = config.get('dest_project') or os.environ.get('GCP_PROJECT')
      client.publish(
        f"projects/{project}/topics/{config['notifier']['topic']}",
        f"{config['notifier'].get('message', 'RUN')}".encode('utf-8'),
        **attributes
      )
      logging.info('Notifying %s of completed job %s.',
                   config['notifier']['topic'], attributes['id'])

    except Exception as e:
      logging.error('Failed to notify %s of completed job %s.',
                    config['notifier']['topic'], attributes['id'])
