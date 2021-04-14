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
import traceback

from importlib import import_module
from typing import Any, Dict

from classes import gmail
from classes.adh import ADH
from classes.cloud_storage import Cloud_Storage
from classes.credentials import Credentials
from classes.dbm_report_runner import DBMReportRunner
from classes.dcm_report_runner import DCMReportRunner
from classes.decorators import measure_memory
from classes.ga360_report_runner import GA360ReportRunner
from classes.postprocessor import PostProcessor
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.sa360_report_manager import SA360Manager
from classes.sa360_report_runner import SA360ReportRunner

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
  if attributes := event.get('attributes'):
    logging.info(attributes)

    try:
      kwargs = {
        'email': attributes.get('email'),
        'project': attributes.get('project'),
        'report_id':
          attributes.get('report_id') or \
            attributes.get('dv360_id') or \
              attributes.get('cm_id'),
        'profile': attributes.get('profile'),
        'sa360_url': attributes.get('sa360_url'),
        'force': attributes.get('force', False),
        'append': attributes.get('append', False),
        'infer_schema': attributes.get('infer_schema', False),
        'dest_project': attributes.get('dest_project'),
        'dest_dataset': attributes.get('dest_dataset', 'report2bq'),
        'notify_topic': attributes.get('notify_topic'),
        'notify_message': attributes.get('notify_message'),
        'partition': attributes.get('partition')
      }
      kwargs.update(attributes)

      if 'type' in attributes: kwargs['product'] = Type(attributes['type'])
      elif kwargs.get('sa360_url'): kwargs['product'] = Type.SA360
      elif kwargs.get('profile'): kwargs['product'] = Type.CM
      else: kwargs['product'] = Type.DV360

      report2bq = Report2BQ(**kwargs)
      report2bq.run()

    except Exception as e:
      if email := attributes.get('email'):
        email_error(email=email,
                    product='Report Fetcher',
                    event=event, error=e)

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

  if attributes := event.get('attributes'):
    T = Type(attributes.get('type'))
    _base_args = {
      'email': attributes.get('email'),
      'project': attributes.get('project', os.environ.get('GCP_PROJECT')),
    }
    if _command := {
      Type.DV360: {
        'runner': DBMReportRunner,
        'args': {
          'dbm_id': attributes.get('dv360_id') or attributes.get('report_id'),
          **_base_args,
        },
      },
      Type.CM: {
        'runner': DCMReportRunner,
        'args': {
          'cm_id': attributes.get('cm_id') or attributes.get('report_id'),
          'profile': attributes.get('profile', None),
          **_base_args,
        }
      },
      Type.SA360_RPT: {
        'runner': SA360ReportRunner,
        'args': {
          'report_id': attributes.get('report_id'),
          'timezone': attributes.get("timezone", None),
          **_base_args,
        }
      },
      Type.ADH: {
        'runner': ADH,
        'args': {
          'adh_customer': lambda: attributes['adh_customer'],
          'adh_query': lambda: attributes['adh_query'],
          'api_key': lambda: attributes['api_key'],
          'days': attributes.get('days', 60),
          'dest_project': attributes.get('dest_project', None),
          'dest_dataset': attributes.get('dest_dataset', None),
          **_base_args,
        }
      },
      Type.GA360_RPT: {
        'runner': GA360ReportRunner,
        'args': {
          'report_id': attributes.get('report_id'),
          **_base_args,
        }
      },
    }.get(T):
      _command['runner'](**_command['args']).run(unattended=True)

    else:
      logging.error('No or unknown report type specified.')


def post_processor(event: Dict[str, Any], context=None) -> None:
  if 'data' in event:
    postprocessor = base64.b64decode(event['data']).decode('utf-8')
    logging.info(f'Loading and running "{postprocessor}"')
    PostProcessor.install_postprocessor()

    if attributes := event.get('attributes'):
      _import = f'import classes.postprocessor.{postprocessor}'
      exec(_import)
      Processor = getattr(
        import_module(f'classes.postprocessor.{postprocessor}'), 'Processor')
      Processor().run(context=context, **attributes)

  else:
    logging.fatal('No postprocessor specified')

def email_error(email: str,
                product: str,
                event: Dict[str, Any],
                error: Exception) -> None:

  body=(
    f'\nError: {error if error else "No exception."}\n\n'
    f'Trace: {gmail.error_to_trace(error)}\n\n'
    f'Event data: {event}'
  )

  message = gmail.GMailMessage(to=[email],
                               subject=f'Error in {product or "Report2BQ"}',
                               project=os.environ.get('GCP_PROJECT'),
                               body=body)

  gmail.send_message(
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
