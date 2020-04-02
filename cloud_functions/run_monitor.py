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
from typing import Dict, List, Any

from classes.dbm import DBM
from classes.dcm import DCM
from classes.firestore import Firestore
from classes.report_type import Type

from google.cloud import bigquery
from google.cloud import pubsub
from google.cloud import storage
from google.cloud.bigquery import LoadJob


class RunMonitor(object):
  """Run the process watching running DV360/CM jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch the Firestore-held list
  list of jobs for running DV360/CM processes. If one is discovered to have completed, the Report2BQ
  process is invoked in the normal manner (via a PubSub message to the trigger queue).

  This process is not 100% necessary; if a report is defined with a "fetcher", then the fetcher will 
  run as usual every hour and will pick up the change anyway. On the other hand, it allows for a user
  to schedule a quick report to run (say) every 30 minutes, and not create a "fetcher" since this process
  takes the "fetcher"'s place.
  """

  firestore = Firestore(in_cloud=True, email=None, project=None)
  CS = storage.Client()
  PS = pubsub.PublisherClient()
  project = None

  def process(self, data: Dict[str, Any], context):
    """[summary]
    
    Arguments:
        data {Dict[str, Any]} -- Data passed in from the calling function, containing the attributes from the
                                 calling PubSub message
        context {} -- unused
    """
    if 'attributes' in data:
      self.project = data['attributes']['project']

      documents = self.firestore.get_all_running()
      for document in documents:
        if document['type'] == Type.DV360:
          self._check_dv360_report(document)
        elif document['type'] == Type.CM:
          self._check_dv360_report(document)
        else:
          logging.error('Invalid report type: {type}'.format(document['type']))
      
    else:
      logging.error('No project specified')


  def _check_dv360_report(self, report: Dict[str, Any]):
    """Check a running DV360 report for completion
    
    Arguments:
        report {Dict[str, Any]} -- The report data structure from Firestore
    """
    dbm = DBM(email=report['email'], project=self.project)
    status = dbm.report_state(report['report_id'])
    config = self.firestore.get_report_config(Type.DV360, report['report_id'])
    append = config['append'] if config and 'append' in config else False

    logging.info('Report {report} status: {status}'.format(report=report['report_id'], status=status))
    if status == 'DONE':
      # Remove job from running
      self.firestore.remove_report_runner(report)

      # Send pubsub to trigger report2bq now
      topic = 'projects/{project}/topics/report2bq-trigger'.format(project=self.project)
      self.PS.publish(topic=topic, data=b'RUN', dv360_id=report['report_id'], email=report['email'], append=append, project=self.project)
    elif status == 'FAILED':
      # Remove job from running
      logging.error('Report {report} failed!'.format(report=report['report_id']))
      self.firestore.remove_report_runner(report)


  def _check_cm_report(self, report: Dict[str, Any]):
    """Check a running CM report for completion
    
    Arguments:
        report {Dict[str, Any]} -- The report data structure from Firestore
    """
    dcm = DCM(email=report['email'], superuser=report['superuser'], project=report['project'])
    config = self.firestore.get_report_config(Type.CM, report['report_id'])
    append = config['append'] if config and 'append' in config else False
    response = dcm.report_state(profile_id=report['profile'], report_id=report['report_id'], file_id=report['file_id'], account_id=report['account_id'])
    status = response['status'] if response and  'status' in response else 'UNKNOWN'

    logging.info('Report {report} status: {status}'.format(report=report['report_id'], status=status))
    if status == 'REPORT_AVAILABLE':
      # Remove job from running
      self.firestore.remove_report_runner(report)

      # Send pubsub to trigger report2bq now
      topic = 'projects/{project}/topics/report2bq-trigger'.format(project=self.project)
      self.PS.publish(topic=topic, data=b'RUN', cm_id=report['report_id'], profile=report['profile'], email=report['email'], append=append, project=self.project)

    elif status == 'FAILED' or status =='CANCELLED':
      # Remove job from running
      logging.error('Report {report} failed!'.format(report=report['report_id']))
      self.firestore.remove_report_runner(status)
