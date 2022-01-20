# Copyright 2022 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import logging
import os
from contextlib import suppress
from typing import Dict, Literal

import jinja2
from flask import Flask, request
from google.cloud import storage

from classes.auth_helper import user
from classes.oauth import OAuth
from classes.report_type import Type
from classes.scheduler import Scheduler
from classes.secret_manager_credentials import Credentials

logging.getLogger().setLevel(logging.INFO)

app = Flask(__name__)
app.config['GCLOUD_PROJECT'] = os.environ['GOOGLE_CLOUD_PROJECT']

template_dir = os.path.join(os.path.dirname(__file__), 'templates')
JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(template_dir),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True
)


@app.after_request
def set_response_headers(response):
  response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
  response.headers['Pragma'] = 'no-cache'
  response.headers['Expires'] = '0'
  return response


# [START index]
@app.route('/', methods=['GET', 'POST'])
def index() -> jinja2.Template:
  """The index method for the appengine.

  Returns:
      Template: The completed html template
  """
  project = os.environ['GOOGLE_CLOUD_PROJECT']
  bucket = f'{project}-report2bq-tokens'

  user_email, user_id = user()

  data = {}

  if creds := Credentials(project=project, email=None):
    template = JINJA_ENVIRONMENT.get_template('index.html')
    running_jobs = Scheduler().process(**{'action': 'list',
                                          'project': project,
                                          'email': user_email})
    jobs = []
    for job in running_jobs:
      with suppress(ValueError, KeyError, TypeError):
        _attrs = job.get('pubsubTarget', {}).get('attributes', {})
        _def = Type(_attrs['type'])
        j = {
            'id': job['name'].split('/')[-1],
            'description': job['description'] if 'description' in
            job else '-- No description given --',
            'type': _def,
            'schedule': job['schedule'],
            'timezone': job['timeZone'],
        }

        j['attributes'] = switch(_def, _attrs)
        jobs.append(j)

    data = {'jobs': jobs, 'user_email': user_email}

  else:
    template = JINJA_ENVIRONMENT.get_template('authenticate.html')
    data = {
        'email': user_email,
        'client_id': creds.project_credentials['web']['client_id'],
    }

  return template.render(data)
# [END index]


@app.route('/authenticate', methods=['GET', 'POST'])
def authenticate() -> jinja2.Template:
  """Runs the OAuth2 authentication flow.

  This calls the OAuth flow for an unknown (but valid) user, or one who is
  reauthenticating.

  Returns:
      Template: the JINJA template
  """
  project = os.environ['GOOGLE_CLOUD_PROJECT']
  bucket = f'{project}-report2bq-tokens'

  user_email, user_id = user()
  template = JINJA_ENVIRONMENT.get_template('authenticate.html')
  creds = Credentials(project=project, email=None)
  data = {
      'email': user_email,
      'client_id': creds.project_credentials['web']['client_id'],
  }
  return template.render(data)


@app.route('/oauth-complete', methods=['POST'])
def oauth_complete() -> Literal['AUTH FAIL: No authentication code received.',
                                'Authenticated!']:
  """Completes the OAuthprocess.

  Returns:
      Literal[str, str]: One of two valid strings.
  """
  return OAuth().oauth_complete(request)


def job_attributes_sa360(attributes: Dict[str, str]) -> Dict[str, str]:
  return {'sa360_url': attributes.get('sa360_url')}


def job_attributes_sa360_report(attributes: Dict[str, str]) -> Dict[str, str]:
  return {'report_id': attributes.get('report_id')}


def job_attributes_ga360_report(attributes: Dict[str, str]) -> Dict[str, str]:
  return {'report_id': attributes.get('report_id')}


def job_attributes_dv360(attributes: Dict[str, str]) -> Dict[str, str]:
  return {
      'report_id': attributes['dv360_id'] or attributes['report_id']
  }


def job_attributes_cm(attributes: Dict[str, str]) -> Dict[str, str]:
  return {
      'report_id': attributes['cm_id'] or attributes['report_id'],
      'profile': attributes['profile']
  }


def job_attributes_adh(attributes: Dict[str, str]) -> Dict[str, str]:
  return {
      'adh_customer': attributes['adh_customer'],
      'adh_query': attributes['adh_query'],
      'api_key': attributes['api_key'],
      'days': attributes['days'],
  }


def switch(report_type: Type, attributes: Dict[str, str]) -> Dict[str, str]:
  job_attribute_extractor = {
      Type.DV360: job_attributes_dv360,
      Type.CM: job_attributes_cm,
      Type.SA360: job_attributes_sa360,
      Type.ADH: job_attributes_adh,
      Type.SA360_RPT: job_attributes_sa360_report,
      Type.GA360_RPT: job_attributes_ga360_report,
  }
  a = {}
  for _attr in [
      'force',
      'rebuild_schema',
      'infer_schema',
      'dest_project',
      'dest_dataset',
      'dest_table'
  ]:
    if extractor := job_attribute_extractor.get(report_type):
      if _attr in attributes:
        a[_attr] = attributes[_attr]
      a.update(extractor(attributes))
  return a


if __name__ == '__main__':
  # This is used when running locally. Gunicorn is used to run the
  # application on Google App Engine. See entrypoint in app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]
