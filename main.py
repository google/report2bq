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

import base64
import logging
import os

from flask import Request
from importlib import import_module, invalidate_caches
from typing import Any, Dict, List, Optional
from urllib.parse import unquote_plus as unquote

from classes.adh import ADH
from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials
from classes.dbm_report_runner import DBMReportRunner
from classes.dcm_report_runner import DCMReportRunner
from classes.decorators import measure_memory
from classes.firestore import Firestore
from classes.gmail import GMail, GMailMessage
from classes.postprocessor import PostProcessor
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.sa360_report_manager import SA360Manager
from classes.sa360_report_runner import SA360ReportRunner
from classes.scheduler import Scheduler

from cloud_functions.job_monitor import JobMonitor
from cloud_functions.run_monitor import RunMonitor
from cloud_functions.report_loader import ReportLoader



@measure_memory
def report_upload(event: Dict[str, Any], context=None):
  ReportLoader().process(event, context)


@measure_memory
def report_fetch(event: Dict[str, Any], context=None):
  """Report fetch request processor

  This is the processor that determines which type of report is to be fetched and in turn
  invokes the Report2BQ process. It scans through the parameters sent from the Cloud Scheduler
  task as part of the PubSub message. These are stored in the 'event' object.

  Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
  """
  if 'attributes' in event:
    attributes = event['attributes']

    try:
      logging.info(f'Attributes: {attributes}')

      kwargs = {
        'email': attributes['email'],
        'project': attributes['project'],
        'report_id': attributes.get('dv360_id') or attributes.get('cm_id') or attributes.get('report_id'),
        'profile': attributes.get('profile', None),
        'sa360_url': attributes.get('sa360_url') if 'sa360_url' in attributes else None,
        'force': attributes.get('force', False),
        'append': attributes.get('append', False),
        'infer_schema': attributes.get('infer_schema', False),
        'dest_project': attributes.get('dest_project', None),
        'dest_dataset': attributes.get('dest_dataset', 'report2bq'),
        'notify_topic': attributes.get('notify_topic', None),
        'notify_message': attributes.get('notify_message', None),
        'partition': attributes.get('partition', False)
      }
      kwargs.update(attributes)

      if 'type' in attributes:
        if attributes['type'] == 'dbm': kwargs['product'] = Type.DV360
        elif attributes['type'] == 'dcm': kwargs['product'] = Type.CM
        else: kwargs['product'] = Type(attributes['type'])
      elif kwargs.get('sa360_url'): kwargs['product'] = Type.SA360
      elif kwargs.get('profile'): kwargs['product'] = Type.CM
      else: kwargs['product'] = Type.DV360

      logging.info(f'args: {kwargs}')
      report2bq = Report2BQ(**kwargs)
      report2bq.run()

    except Exception as e:
      if 'email' in attributes:
        email_error(email=attributes['email'], product='Report Fetcher', event=event, error=e)

      logging.fatal(f'Error: {e}')
      return


def job_monitor(event: Dict[str, Any], context=None):
  """Run the process watching running Big Query import jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch a queue (held in Firestore)
  for running BigQuery csv import jobs started as a result of the Report2BQ process. Upon successful
  completion, it deletes the file from Cloud Storage. If this job is not run, the last file will remain in
  GCS.

  Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
  """
  JobMonitor().process(event, context)


def run_monitor(event: Dict[str, Any], context=None):
  """Run the process watching running DV360/CM jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch the Firestore-held list
  list of jobs for running DV360/CM processes. If one is discovered to have completed, the Report2BQ
  process is invoked in the normal manner (via a PubSub message to the trigger queue).

  This process is not 100% necessary; if a report is defined with a "fetcher", then the fetcher will
  run as usual every hour and will pick up the change anyway. On the other hand, it allows for a user
  to schedule a quick report to run (say) every 30 minutes, and not create a "fetcher" since this process
  takes the "fetcher"'s place.

  Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
  """
  try:
    RunMonitor().process({}, None)

  except Exception as e:
    logging.error(e)


def report_runner(event: Dict[str, Any], context=None):
  """Run a DV360, CM, SA360 or ADH report on demand

  This allows a user to issue the API-based run report directive to start unscheduled, unschedulable (ie
  today-based) or simply control the run time of DV360/CM and ADH reports. A job kicked off using this process
  will be monitored by the "run-monitor", or can simply be left if a "fetcher" is enabled.

  Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
  """
  email = None

  if 'attributes' in event:
    attributes = event['attributes']
    try:
      logging.info(attributes)
      if 'type' in attributes:
        if Type(attributes['type']) == Type.DV360:
          dv360_id = attributes.get('dv360_id') or attributes.get('report_id')
          email = attributes['email']
          project = attributes['project'] or os.environ.get('GCP_PROJECT')

          runner = DBMReportRunner(
            dbm_id=dv360_id,
            email=email,
            project=project
          )

        elif Type(attributes['type']) == Type.CM:
          cm_id = attributes.get('cm_id') or attributes.get('report_id')
          profile = attributes.get('profile', None)
          email = attributes['email']
          project = attributes['project'] or os.environ.get('GCP_PROJECT')

          runner = DCMReportRunner(
            cm_id=cm_id,
            profile=profile,
            email=email,
            project=project
          )

        elif Type(attributes['type']) == Type.SA360_RPT:
          report_id = attributes['report_id']
          email = attributes['email']
          project = attributes['project'] or os.environ.get('GCP_PROJECT')
          timezone = attributes.get("timezone", None)
          runner = SA360ReportRunner(
            report_id=report_id,
            email=email,
            project=project,
            timezone=timezone
          )


        elif Type(attributes['type']) == Type.ADH:
          adh_customer = attributes['adh_customer']
          adh_query = attributes['adh_query']
          api_key = attributes['api_key']
          email = attributes['email']
          project = attributes['project'] or os.environ.get('GCP_PROJECT')
          days = attributes.get('days') if 'days' in attributes else 60
          dest_project = attributes.get('dest_project') if 'dest_project' in attributes else None
          dest_dataset = attributes.get('dest_dataset') if 'dest_dataset' in attributes else None

          # Always run this as async: forcing to be certain
          runner = ADH(
            email=email,
            project=project,
            adh_customer=adh_customer,
            adh_query=adh_query,
            api_key=api_key,
            days=days,
            dest_project=dest_project,
            dest_dataset=dest_dataset
          )

        else:
          logging.error('Invalid report type specified: {type}'.format(type=attributes['type']))
          return

        runner.run(unattended=True)

      else:
        logging.error('No report type specified.')

    except Exception as e:
      if email:
        email_error(email=email, product="report_runner", event=event, error=e)

      logging.fatal(f'Error: {e}\Event Data supplied: {event}')
      return


def post_processor(event: Dict[str, Any], context=None):
  logging.info(f'Event supplied: {event}')
  if 'data' in event:
    postprocessor = base64.b64decode(event['data']).decode('utf-8')
    logging.info(f'Loading and running "{postprocessor}"')
    PostProcessor.install_postprocessor()

  else:
    logging.fatal('No postprocessor specified')
    return

  if 'attributes' in event:
    attributes = event['attributes']
    _import = f'import classes.postprocessor.{postprocessor}'
    exec(_import)
    Processor = getattr(import_module(f'classes.postprocessor.{postprocessor}'), 'Processor')

    Processor().run(context=context, **attributes)


def email_error(email: str, product: str, event: Dict[str, Any], error: Exception):
  message = GMailMessage(
    to=[email],
    subject=f'Error in {product or "Report2BQ"}',
    body=f'''
Error: {error if error else 'No exception.'}

Event data: {event}
''',
    project=os.environ.get('GCP_PROJECT'))

  GMail().send_message(
    message=message,
    credentials=Credentials(email=email, project=os.environ.get('GCP_PROJECT'))
  )


def sa360_report_manager(event: Dict[str, Any], context=None) -> None:
    """Process a file added to the sa360_report_manager bucket.

    Arguments:
        event {Dict[str, Any]} -- data sent from the PubSub message
        context {Dict[str, Any]} -- context data. unused
    """
    logging.info(event)
    project = os.environ.get('GCP_PROJECT')

    bucket_name = event['bucket']
    file_name = event['name']
    *n, e = file_name.split('/')[-1].split('.')
    (name, extension) = ('.'.join(n).lower(), e.lower())

    logging.info('Processing file %s', file_name)
    try:
      args = {
        'report': name,
        'project': project,
        'file': file_name,
        'gcs_stored': True,
        'action': extension,
      }
      SA360Manager().manage(**args)
      Cloud_Storage.rename(
        bucket=bucket_name,
        source=file_name, destination=f'{file_name}.processed')

    except NotImplementedError:
      logging.debug(
        'Extension command %s is not a valid action. Ignoring.', extension)
      return
