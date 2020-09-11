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

from absl import app, flags

from classes.firestore import Firestore
from classes.report_type import Type
from classes.sa360_report_runner import SA360ReportRunner
from classes.sa360_v2 import SA360
from classes.scheduler import Scheduler
from contextlib import suppress

from google.cloud.scheduler import CloudSchedulerClient
from google.cloud import client

"""
Currently Non functional, this can be used to manually install the json for SA360 Dynamic Reports.

Better system coming.
"""
FLAGS = flags.FLAGS

def main(unused_argv):
  _scheduler = CloudSchedulerClient()
  j = _scheduler.list_jobs(parent='projects/report2bq-zz9-plural-z-alpha')

  location_path('report2bq-test-bb', 'us-central1')
  _jobs = _scheduler.list_jobs(parent=location_path('report2bq-test-bb', 'us-central1'))
  
  _job = _scheduler.get_job(name=_scheduler.job_path('report2bq-test-bb', 'us-central1', 'run-dv360-727664629'))

  print(_job)

def job_path(project, location, job):
    """Return a fully-qualified job string."""
    return f"projects/{project}/locations/{location}/jobs/{job}"


def location_path(project, location):
    """Return a fully-qualified location string."""
    return f"projects/{project}/locations/{location}"


def project_path(project):
    """Return a fully-qualified project string."""
    return f"projects/{project}"


if __name__ == '__main__':
  with suppress(SystemExit):
    app.run(main)
