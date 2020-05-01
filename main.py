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

import logging
import os

from typing import Dict, Any

from cloud_functions.job_monitor import JobMonitor
from cloud_functions.run_monitor import RunMonitor
from cloud_functions.oauth import OAuth
from cloud_functions.report_loader import ReportLoader
from classes.adh import ADH
from classes.dbm_report_runner import DBMReportRunner
from classes.dcm_report_runner import DCMReportRunner
from classes.report2bq import Report2BQ
from classes.report_type import Type
from classes.decorators import measure_memory
from flask import Request


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
      logging.info(attributes)
      email = attributes['email']
      project = attributes['project']
      dv360_id = attributes.get('dv360_id', None)
      cm_id = attributes.get('cm_id', None)
      sa360_url = attributes.get('sa360_url') if 'sa360_url' in attributes else None
      profile = attributes.get('profile', None)
      cm_superuser = attributes.get('cm_superuser', False)
      account = attributes.get('account', None)
      if cm_superuser and not account:
        raise "Superuser must specify account as well as profile"
      rebuild_schema = attributes.get('rebuild_schema', False)
      force = attributes.get('force', False)
      append = attributes.get('append', False)
      infer_schema = attributes.get('infer_schema', False)
      dest_project = attributes.get('dest_project', None)
      dest_dataset = attributes.get('dest_dataset', 'report2bq')

    except Exception as e:
      logging.fatal('Error: {e}\nMissing mandatory attributes: {attributes}'.format(e=e, attributes=attributes))
      return

    fetcher = Report2BQ(
      dv360=True if dv360_id else False,
      dv360_id=dv360_id,
      cm=True if cm_id else False,
      cm_id=cm_id,
      sa360=True if sa360_url else False,
      sa360_url=sa360_url,
      rebuild_schema=rebuild_schema,
      force=force,
      profile=profile,
      superuser=cm_superuser,
      account_id=account,
      email=email,
      append=append,
      project=project,
      infer_schema=infer_schema,
      dest_project=dest_project,
      dest_dataset=dest_dataset
    )
    fetcher.run()


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
  RunMonitor().process(event, context)


def report_runner(event: Dict[str, Any], context=None):
  """Run a DV360/CM or ADH report on demand

  This allows a user to issue the API-based run report directive to start unscheduled, unschedulable (ie 
  today-based) or simply control the run time of DV360/CM and ADH reports. A job kicked off using this process
  will be monitored by the "run-monitor", or can simply be left if a "fetcher" is enabled.
  
  Arguments:
      event {Dict[str, Any]} -- data sent from the PubSub message
      context {Dict[str, Any]} -- context data. unused
  """
  if 'attributes' in event:
    attributes = event['attributes']
    try:
      logging.info(attributes)
      if 'type' in attributes:
        if Type(attributes['type']) == Type.DBM:
          dv360_ids = [attributes.get('dv360_id')] if 'dv360_id' in attributes else None
          email = attributes['email']
          project = attributes['project'] or os.environ.get('GCP_PROJECT')

          runner = DBMReportRunner(
            dbm_ids=dv360_ids,
            email=email,
            synchronous=False,
            project=project
          )

        elif Type(attributes['type']) == Type.DCM:
          cm_ids = [attributes.get('cm_id')] if 'cm_id' in attributes else None
          profile = attributes.get('profile', None)
          cm_superuser = attributes.get('cm_superuser', False)
          account = attributes.get('account', None)
          if cm_superuser and not account:
            raise "Superuser must specify account as well as profile"
          email = attributes['email']
          project = attributes['project'] or os.environ.get('GCP_PROJECT')

          runner = DCMReportRunner(
            cm_ids=cm_ids,
            profile=profile,
            superuser=cm_superuser,
            account_id=account,
            email=email,
            synchronous=False,
            project=project
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
      logging.fatal('Error: {e}\nAttributes supplied: {attributes}'.format(e=e, attributes=attributes))
      return


def oauth_request(request: Request) -> str:
  page = f"""
<head></head>
<body>
<h1>OAuth token generation</h1>
<p/>
<form action="/OAuth" type="POST">
  <label for="email">Please enter your email address:</label><input type="text" id="email" name="email" /><br/>
  <input type="hidden" name="project" value="{os.environ.get('GCP_PROJECT')}" />
  <input type="submit" value="SUBMIT REQUEST"></input>
</form>
</body>
"""
  return page


def oauth(request: Request) -> str:
  logging.info(request.args)
  project = request.args.get('project', type=str)
  email = request.args.get('email', type=str)
  o = OAuth()
  return o.oauth_init(request, project, email)


def oauth_complete(request: Request) -> str:
  logging.info(request.args)
  o = OAuth()
  o.oauth_complete(request)