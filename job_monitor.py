"""
  while not status['status']['state'] == 'DONE':
    show_message('Waiting for job %s to complete' % job_id)
    time.sleep(5)
    status = bq.jobs().get(projectId=FLAGS.project, jobId=job_id).execute()
"""

import json

from classes.bigquery import BigQuery
from classes.firestore import Firestore
from google.cloud.bigquery import LoadJob
from cloud_functions.job_monitor import JobMonitor

def main(args: []):
  JobMonitor().process(data=None, context=None)

if __name__ == '__main__':
  main([])
