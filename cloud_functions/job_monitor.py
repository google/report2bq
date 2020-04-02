"""
Copyright 2018 Google LLC

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
import re

from absl import app
from typing import Dict, Any
from classes.firestore import Firestore
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJob


class JobMonitor(object):
  """The process watching running Big Query import jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch a queue (held in Firestore)
  for running BigQuery csv import jobs started as a result of the Report2BQ process. Upon successful
  completion, it deletes the file from Cloud Storage. If this job is not run, the last file will remain in
  GCS.

  Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
  """
  firestore = Firestore(in_cloud=True, email=None, project=None)
  bq = bigquery.Client()
  CS = storage.Client()


  def process(self, data: Dict[str, Any], context):
    """Check all the running jobs
    
    Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
    """
    documents = self.firestore.get_all_jobs()
    for document in documents:
      api_repr = document.get().to_dict()
      job = LoadJob.from_api_repr(api_repr, self.bq)
      job.reload()

      if job.state == 'DONE':
        if job.error_result:
          logging.error(job.errors)

        self.firestore.mark_import_job_complete(document.id, job)
        self._handle_finished(job=job)


  def _handle_finished(self, job: LoadJob):
    """Deal with completed jobs

    When we find a completed job, delete the source CSV from GCS.
    
    Arguments:
        job {LoadJob} -- Big Query import job
    """
    for source in job.source_uris:
      match = re.match(r'gs://([^/]+)/(.*)', source)

      bucket_name = match[1]
      blob_name = match[2]

      source_bucket = self.CS.get_bucket(bucket_name)
      source_blob = source_bucket.blob(blob_name)

      source_blob.delete()

      logging.info('File {file} removed from {source}.'.format(file=blob_name, source=bucket_name))
 