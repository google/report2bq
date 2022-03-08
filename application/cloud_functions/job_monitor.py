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
import logging
import os
import re

from classes import secret_manager_credentials as credentials
from classes import decorators
from classes.abstract_datastore import AbstractDatastore
from classes.firestore import Firestore
from classes.report_type import Type
from concurrent import futures

from google.cloud import bigquery
from google.cloud import pubsub
from google.cloud import storage
from google.cloud.bigquery import LoadJob
from typing import Any, Dict


class JobMonitor(object):
  """The process watching running Big Query import jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch
  a queue (held in Firestore) for running BigQuery csv import jobs started as a
  result of the Report2BQ process. Upon successful completion, it deletes the
  file from Cloud Storage. If this job is not run, the last file will remain
  in GCS.
  """
  @decorators.lazy_property
  def firestore(self) -> AbstractDatastore:
    return Firestore()

  def process(self, data: Dict[str, Any], context) -> None:
    """Checks all the running jobs.

    Args:
      event (Dict[str, Any]):  data sent from the PubSub message
      context (Dict[str, Any]):  context data. unused
    """
    attributes = data.get('attributes')
    documents = self.firestore.get_all_documents(Type._JOBS)

    for document in documents:
      for product in [T for T in Type]:
        if config := self.firestore.get_document(product, document.id):
          if config.get('dest_project'):
            user_creds = \
                credentials.Credentials(email=config['email'],
                                        project=config['dest_project'])
            bq = bigquery.Client(project=config['dest_project'],
                                 credentials=user_creds.credentials)

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
                    report_type=product, config=config, job=job, id=document.id)
                self._mark_import_job_complete(document.id, job,)

            except Exception as e:
              logging.error(
                  'Error loading job %s for monitoring.', document.id)

          break

  def _handle_finished(self, job: LoadJob, config: Dict[str, Any]) -> None:
    """Deals with completed jobs.

    When we find a completed job, delete the source CSV from GCS.

    Args:
        job (LoadJob):  Big Query import job
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

  def _mark_import_job_complete(self, report_id: int,
                                job: bigquery.LoadJob) -> None:
    """Marks a BQ Import job in Firestore done.

    Moves an import job from 'jobs/' to 'jobs-completed'.

    Args:
        report_id (int): [description]
        job (bigquery.LoadJob): [description]
    """
    self.firestore.delete_document(Type._JOBS, report_id)
    self.firestore.store_document(Type._COMPLETED, report_id, job.to_api_repr())

  def notify(self,
             report_type: Type,
             config: Dict[str, Any],
             job: LoadJob, id: str) -> None:
    """Notifies the postprocessor.

    Kicks the approriate postprocessor into action.

    Args:
        report_type (Type): the report type
        config (Dict[str, Any]): the report config
        job (LoadJob): the BQ load job config
        id (str): the BQ job id
    """
    columns = ';'.join([field['name'] for field in config['schema']])

    attributes = {
        'project': job.destination.project,
        'dataset': job.destination.dataset_id,
        'table': job.destination.table_id,
        'rows': str(job.output_rows),
        'id': id,
        'type': report_type.value,
        'columns': columns
    }

    logging.info('Notifying postprocessor of completed job %s.',
                 attributes['id'])
    client = pubsub.PublisherClient()
    try:
      project = os.environ.get('GCP_PROJECT')
      futures.wait([
          client.publish(
              (f"projects/{project}/topics/"
               f"{os.environ.get('POSTPROCESSOR', 'report2bq-postprocessor')}"),
              f"{config['notifier'].get('message', 'report2bq_unknown')}"
              .encode('utf-8'),
              **attributes)],
          return_when=futures.ALL_COMPLETED)

    except Exception as e:
      logging.error('Failed to notify postprocessor of completed job %s.',
                    attributes['id'])
