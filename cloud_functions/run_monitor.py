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
import re

from absl import app
from typing import Dict, List, Any
from contextlib import suppress

from classes.dbm import DBM
from classes.dcm import DCM
from classes.sa360_v2 import SA360
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

  firestore = Firestore()
  PS = pubsub.PublisherClient()

  def process(self, data: Dict[str, Any], context):
    """[summary]
    
    Arguments:
        data {Dict[str, Any]} -- Data passed in from the calling function, containing the attributes from the
                                 calling PubSub message
        context {} -- unused
    """
    self.project = os.environ['GCP_PROJECT']
    report_checker = {
      Type.DV360: self._check_dv360_report,
      Type.CM: self._check_cm_report,
      Type.SA360: self._check_sa360_report,
      Type.SA360_RPT: self._check_sa360_report
    }

    try:
      documents = self.firestore.get_all_running()
      for document in documents:
        with suppress(ValueError):
          run_config = document.get().to_dict()
          T = Type(run_config['type'])
          config = self.firestore.get_report_config(T, document.id)
          if config: 
            report_checker[T](config, run_config)
            break
          else:
            logging.error(f'Invalid report: {document.get().to_dict()}')

    except Exception as e:
      logging.error(e)


  def _check_dv360_report(self, config: Dict[str, Any], run_config: Dict[str, Any]):
    """Check a running DV360 report for completion
    
    Arguments:
        report {Dict[str, Any]} -- The report data structure from Firestore
    """
    dbm = DBM(email=config['email'], project=self.project)
    status = dbm.report_state(config['id'])
    append = config['append'] if config and 'append' in config else False

    logging.info('Report {report} status: {status}'.format(report=config['id'], status=status))
    if status == 'DONE':
      # Remove job from running
      self.firestore.remove_report_runner(config['id'])

      # Send pubsub to trigger report2bq now
      topic = 'projects/{project}/topics/report2bq-trigger'.format(project=self.project)
      self.PS.publish(
        topic=topic,
        data=b'RUN',
        report_id=config['id'],
        email=config['email'],
        append=str(append),
        project=self.project
      )
    elif status == 'FAILED':
      # Remove job from running
      logging.error('Report {report} failed!'.format(report=config['id']))
      self.firestore.remove_report_runner(config['id'])


  def _check_cm_report(self, config: Dict[str, Any], run_config: Dict[str, Any]):
    """Check a running CM report for completion
    
    Arguments:
        report {Dict[str, Any]} -- The report data structure from Firestore
    """
    dcm = DCM(email=config['email'], project=self.project, profile=config['profile_id'])
    append = config['append'] if config and 'append' in config else False
    response = dcm.report_state(report_id=config['id'], file_id=config['report_file']['id'])
    status = response['status'] if response and  'status' in response else 'UNKNOWN'

    logging.info('Report {report} status: {status}'.format(report=config['id'], status=status))
    if status == 'REPORT_AVAILABLE':
      # Remove job from running
      self.firestore.remove_report_runner(config['id'])

      # Send pubsub to trigger report2bq now
      topic = 'projects/{project}/topics/report2bq-trigger'.format(project=self.project)
      self.PS.publish(
        topic=topic, data=b'RUN',
        report_id=config['id'],
        profile=config['profile_id'],
        email=config['email'],
        append=str(append),
        project=self.project)

    elif status == 'FAILED' or status =='CANCELLED':
      # Remove job from running
      logging.error('Report {report} failed!'.format(report=config['id']))
      self.firestore.remove_report_runner(config['id'])


  def _check_sa360_report(self, config: Dict[str, Any], run_config: Dict[str, Any]): 
    sa360 = SA360(email=run_config['email'], project=self.project)
    
    if sa360.handle_offline_report(run_config=run_config):
      self.firestore.remove_report_runner(run_config['report_id'])
      logging.info(f'Report {run_config["report_id"]} done.')

    else:
      # SA360 ones can't fail - they won't start if there are errors, so it's just
      # not ready yet. So just leave it here and try again later.
      logging.error(f'Report {run_config["report_id"]} not ready.')
