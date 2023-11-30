# Copyright 2021 Google LLC
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

import datetime
import logging
import os
from typing import Any, Dict

from auth import credentials
from auth.datastore.secret_manager import SecretManager
from classes import decorators, firestore, gmail
from classes.dbm import DBM
from classes.dcm import DCM
from classes.report_type import Type
from classes.scheduler import Scheduler
from concurrent import futures

from google.cloud import pubsub
from google.cloud.scheduler import Job


class RunMonitor(object):
  """Run the process watching running DV360/CM jobs

  This process is triggered by a Cloud Scheduler job every 5 minutes to watch
  the Firestore-held list list of jobs for running DV360/CM processes. If one
  is discovered to have completed, the Report2BQ process is invoked in the
  normal manner (via a PubSub message to the trigger queue).

  This process is not 100% necessary; if a report is defined with a "fetcher",
  then the fetcher will run as usual every hour and will pick up the change
  anyway. On the other hand, it allows for a user to schedule a quick report to
  run (say) every 30 minutes, and not create a "fetcher" since this process
  takes the "fetcher"'s place.
  """

  @decorators.lazy_property
  def firestore_client(self) -> firestore.Firestore:
    """The Firestore client."""
    return firestore.Firestore()

  @decorators.lazy_property
  def pubsub_client(self) -> pubsub.PublisherClient:
    """The pubsub client"""
    return pubsub.PublisherClient()

  def remove_report_runner(self, id: str) -> None:
    """Deletes a report runner from the datastore

    Args:
        id (str): the report runner's id
    """
    self.firestore_client.delete_document(Type._RUNNING, id=id)

  def update_report_runner(self, id: str, status: str) -> None:
    """Updates a report runner in the datastore

    Args:
        id (str): the report runner's id
    """
    self.firestore_client.update_document(
        Type._RUNNING,
        id=id,
        new_data={
            'status': status,
            'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S%z')
        })

  def process(self, data: Dict[str, Any], context) -> None:
    """Execute the run_monitor.

    Arguments:
        data (Dict[str, Any]):  Data passed in from the calling function,
                                 containing the attributes from the
                                 calling PubSub message
        context ():  unused
    """
    self.project = os.environ['GCP_PROJECT']
    report_checker = {
        Type.DV360: self._check_dv360_report,
        Type.CM: self._check_cm_report,
        Type.SA360: self._check_sa360_report,
        Type.SA360_RPT: self._check_sa360_report
    }

    documents = list(self.firestore_client.get_all_documents(Type._RUNNING))
    logging.info('To process: %s', ','.join([d.id for d in documents]))
    for document in documents:
      run_config = document.get().to_dict()
      T = Type(run_config.get('type'))
      (success, job_config) = self._fetch_schedule(type=T,
                                                   run_config=run_config)

      try:
        if success:
          logging.info('Processing job %s', run_config['report_id'])
          match T:
            case Type.DV360: self._check_dv360_report(
                run_config=run_config, job_config=job_config)
            case Type.CM: self._check_cm_report(
                run_config=run_config, job_config=job_config)
            case Type.SA360: self._check_sa360_report(
                run_config=run_config, job_config=job_config)
            case Type.SA360_RPT: self._check_sa360_report(
                run_config=run_config, job_config=job_config)
            case _: self._invalid_type(
                run_config=run_config, job_config=job_config)
        else:
          # Invalid job; remove this runner
          self.remove_report_runner(document.id)

      except Exception as e:
        logging.error(gmail.error_to_trace(e))
        self._email_error(
            message=(
                f'Error in run monitor for job {run_config["report_id"]}. '
                f'Report config is: {job_config}'),
            error=e)

  def _fetch_schedule(self,
                      type: Type,
                      run_config: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches the job schedule.

    Args:
        type (Type): the typ(product) of the job.
        run_config (Dict[str, Any]): the configuration.

    Returns:
        Dict[str, Any]: the schedule details.
    """
    scheduler = Scheduler()
    return scheduler.process(
        action='get',
        project=os.environ['GCP_PROJECT'],
        email=run_config.get('email'),
        html=False,
        job_id=type.runner(run_config.get('report_id'))
    )

  def _invalid_type(self,
                    job_config: Dict[str, Any],
                    run_config: Dict[str, Any]) -> None:
    """Invalid job type handler.

    This shouldn't even happen (but could if somebody manually messes with
    the Firestore), but is handled just in case.

    Args:
        job_config (Dict[str, Any]): job configuration
        run_config (Dict[str, Any]): current run configuration
    """
    raise NotImplementedError('Invalid job type requested')

  def _check_dv360_report(self,
                          job_config: Job,
                          run_config: Dict[str, Any]) -> None:
    """Check a running DV360 report for completion.

    Args:
        job_config (Dict[str, Any]): job configuration
        run_config (Dict[str, Any]): current run configuration
    """
    job_attributes = job_config.pubsub_target.attributes
    dbm = DBM(email=job_attributes['email'], project=job_attributes['project'])
    status = dbm.report_state(run_config['report_id'])

    logging.info('Report %s status: %s', run_config['report_id'], status)

    if status == 'DONE':
      # Send pubsub to trigger report2bq now
      topic = f'projects/{self.project}/topics/report2bq-fetcher'
      futures.wait([self.pubsub_client.publish(
          topic=topic,
          data=b'RUN',
          **job_attributes)], return_when=futures.ALL_COMPLETED)

      # Remove job from running
      self.remove_report_runner(run_config['report_id'])

    elif status == 'FAILED':
      # Remove job from running
      logging.error(f'Report %s failed!', run_config['report_id'])
      self.remove_report_runner(run_config['report_id'])

    else:
      self.update_report_runner(run_config['report_id'], status)

  def _check_cm_report(self,
                       job_config: Job,
                       run_config: Dict[str, Any]) -> None:
    """Check a running CM360 report for completion.

    Args:
        job_config (Dict[str, Any]): job configuration
        run_config (Dict[str, Any]): current run configuration
    """
    job_attributes = job_config.pubsub_target.attributes
    dcm = DCM(email=job_attributes['email'],
              project=job_attributes['project'],
              profile=job_attributes['profile'])
    response = dcm.report_state(report_id=run_config['report_id'],
                                file_id=run_config['file_id'])
    status = \
        response['status'] if response and 'status' in response else 'UNKNOWN'

    logging.info('Report %s status: %s.', run_config['report_id'], status)
    if status == 'REPORT_AVAILABLE':
      # Send pubsub to trigger report2bq now
      topic = f'projects/{self.project}/topics/report2bq-fetcher'
      futures.wait([self.pubsub_client.publish(
          topic=topic,
          data=b'RUN',
          **job_attributes)], return_when=futures.ALL_COMPLETED)

      # Remove job from running
      self.remove_report_runner(run_config['report_id'])

    elif status == 'FAILED' or status == 'CANCELLED':
      # Remove job from running
      logging.error('Report %s failed!', run_config['report_id'])
      self.remove_report_runner(run_config['report_id'])

    else:
      self.update_report_runner(run_config['report_id'], status)

  def _check_sa360_report(self,
                          job_config: Job,
                          run_config: Dict[str, Any]) -> None:
    """Check a running SA360 report for completion.

    Args:
        job_config (Dict[str, Any]): job configuration
        run_config (Dict[str, Any]): current run configuration
    """
    # Merge configs
    job_attributes = job_config.pubsub_target.attributes
    config = {**run_config, **job_attributes}

    # Send pubsub to trigger report2bq now
    topic = f'projects/{self.project}/topics/report2bq-fetcher'
    futures.wait([self.pubsub_client.publish(
        topic=topic,
        data=b'RUN',
        **config)], return_when=futures.ALL_COMPLETED)

  def _email_error(self,
                   message: str,
                   email: str = None,
                   error: Exception = None) -> None:
    """Emails the error details.

    Emails an error and the stack trace to the job owner and the administrator,
    if they are defined.

    Args:
        message (str): a text message.
        email (str, optional): the job owner's email. Defaults to None.
        error (Exception, optional): an exception. Defaults to None.
    """
    to = [email] if email else []
    administrator = \
        os.environ.get('ADMINISTRATOR_EMAIL') or \
        self.FIRESTORE.get_document(Type._ADMIN, 'admin').get('email')
    cc = [administrator] if administrator else []
    body = f'{message}{gmail.error_to_trace(error)}'

    if to or cc:
      message = gmail.GMailMessage(
          to=to,
          cc=cc,
          subject=f'Error in run-monitor',
          body=body,
          project=os.environ.get('GCP_PROJECT'))

      gmail.send_message(
          message=message,
          credentials=credentials.Credentials(datastore=SecretManager,
                                              email=email or administrator, project=os.environ.get('GCP_PROJECT'))
      )
