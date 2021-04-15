# Copyright 2020 Google LLC
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

import json
from oauth2client import client
import web

SECRETS_FILE = 'tokens/client_secrets.json'

# ###############################################
# DO NOT EDIT BELOW HERE
# ###############################################

urls = (
    '/', 'Index',
    '/oauth-response', 'Consented',
)

with open(SECRETS_FILE, 'r') as secret_file:
  secrets = json.loads(secret_file.read())
  flow = client.OAuth2WebServerFlow(
      client_id=secrets['web']['client_id'],
      client_secret=secrets['web']['client_secret'],
      scope=[
        'https://www.googleapis.com/auth/analytics',
        'https://www.googleapis.com/auth/adsdatahub',
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/datastore',
        'https://www.googleapis.com/auth/dfareporting',
        'https://www.googleapis.com/auth/devstorage.read_write',
        'https://www.googleapis.com/auth/doubleclickbidmanager',
        'https://www.googleapis.com/auth/doubleclicksearch',
        'https://www.googleapis.com/auth/gmail.send',
      ],
      redirect_uri='http://localhost:8080/oauth-response',
      prompt='consent',
  )


class Index(object):
  """
  Handles response to user request for root page.
  """

  def GET(self):

    # Create redirect url
    auth_url = flow.step1_get_authorize_url()

    # Redirect to URL
    web.seeother(auth_url)


class Consented(object):
  """
  Handles response for /consent, redirects to oauth2.
  """

  def GET(self):
    # Response code
    response_code = web.input().code

    # Get credentials
    credentials = flow.step2_exchange(response_code)

    # Save File
    user_token_file = 'user_token.json'
    with open(user_token_file, 'w') as token_file:
      token_file.write(json.dumps(credentials_to_dict(credentials)))

    # Message

    message = (f'Your JSON Token has been stored in {user_token_file}.<p/>'
              'You may now end the Python process.')

    return message


def credentials_to_dict(credentials):
  return {
           'access_token': credentials.access_token,
           'refresh_token': credentials.refresh_token,
         }


# web.py boiler plate
if __name__ == '__main__':
  app = web.application(urls, globals())
  app.run()
