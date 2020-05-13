# Copyright 2019 Google, LLC.
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
import base64
import jinja2
import json
import logging
import os

from flask import current_app, Flask, render_template, request
from google.auth.transport import requests
from google.cloud import pubsub_v1
from google.oauth2 import id_token
from google.cloud import storage

from report2bq.auth_helper import user
from report2bq import oauth


logging.getLogger().setLevel(logging.INFO)

app = Flask(__name__)

# Configure the following environment variables via app.yaml
# This is used in the push request handler to verify that the request came from
# pubsub and originated from a trusted source.
# app.config['PUBSUB_VERIFICATION_TOKEN'] = os.environ['PUBSUB_VERIFICATION_TOKEN']
# app.config['PUBSUB_TOPIC'] = os.environ['PUBSUB_TOPIC']
app.config['GCLOUD_PROJECT'] = os.environ['GOOGLE_CLOUD_PROJECT']

template_dir = os.path.join(os.path.dirname(__file__), 'templates')
JINJA_ENVIRONMENT = jinja2.Environment(
    loader = jinja2.FileSystemLoader(template_dir),
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
def index():
  project = os.environ['GOOGLE_CLOUD_PROJECT']
  bucket = f'{project}-report2bq-tokens'
  project_credentials = json.loads(oauth.OAuth.fetch_file(
    bucket,
    'client_secrets.json'
  ), encoding='utf-8')

  user_email, user_id = user()

  client = storage.Client(credentials=None)
  has_auth = client.get_bucket(bucket).get_blob(f'{user_email}_user_token.json')
  data = {}

  if has_auth:
    template = JINJA_ENVIRONMENT.get_template('index.html')
  
  else:
    template = JINJA_ENVIRONMENT.get_template('authenticate.html')
    data = {
      'email': user_email,
      'client_id': project_credentials['web']['client_id'],
    }

  return template.render(data)
# [END index]


@app.route('/authenticate', methods=['GET', 'POST'])
def authenticate():
  project = os.environ['GOOGLE_CLOUD_PROJECT']
  bucket = f'{project}-report2bq-tokens'
  project_credentials = json.loads(oauth.OAuth.fetch_file(
    bucket,
    'client_secrets.json'
  ), encoding='utf-8')

  user_email, user_id = user()
  template = JINJA_ENVIRONMENT.get_template('authenticate.html')
  data = {
    'email': user_email,
    'client_id': project_credentials['web']['client_id'],
  }
  return template.render(data)


@app.route('/oauth-complete', methods=['POST'])
def oauth_complete():
  return oauth.OAuth().oauth_complete(request)


if __name__ == '__main__':
  # This is used when running locally. Gunicorn is used to run the
  # application on Google App Engine. See entrypoint in app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]
