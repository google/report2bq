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

# Python Imports
import json
import logging

# Oauth Import
import google.auth.transport.requests
import google.oauth2.credentials

# Class Imports
from classes.files import Files
from classes.cloud_storage import Cloud_Storage
from google.cloud import storage


class Credentials(object):
  """Credentials handler
  """
  creds = None

  def __init__(self, in_cloud: bool=True, email: str=None, project: str=None):
    """
    Initialize Credential Class
    """
    if not self.creds:
      if in_cloud:
        self.project_credentials = json.loads(
          Cloud_Storage.fetch_file(
            '{project}-report2bq-tokens'.format(project=project),
            'client_secrets.json'
          )
        )
        self.token_details = json.loads(
          Cloud_Storage.fetch_file(
            '{project}-report2bq-tokens'.format(project=project), 
            '{email}_user_token.json'.format(email=email)
          )
        )

      else:
        # File paths
        project_credentials_path = Files.get_file_path('/config_files/client_secrets.json')
        token_details_path = Files.get_file_path('/config_files/user_token_details.json')

        # Load credential details
        with open(project_credentials_path) as file:
          self.project_credentials = json.load(file)

        # Load Token details
        with open(token_details_path) as file:
          self.token_details = json.load(file)

      # Remove top-level element
      secrets = self.project_credentials['web'] if 'web' in self.project_credentials else self.project_credentials['installed']

      # Init credentials
      self.creds = google.oauth2.credentials.Credentials(
          None,
          refresh_token = self.token_details['refresh_token'],
          token_uri = "https://accounts.google.com/o/oauth2/token",
          client_id = secrets['client_id'],
          client_secret = secrets['client_secret']
      )

      # Force Refresh token
      self.creds.refresh(
          google.auth.transport.requests.Request()
      )


  def get_credentials(self):
    """
    Return credentials
    Returns:
       credential object
    """

    # Return
    return self.creds


  def get_auth_headers(self):
    """
    Returns authorized http headers
    Returns:
      OAuth authenticated headers
    """

    # Oauth Headers
    oauth2_header = {}

    # Apply credential to headers
    self.creds.apply(oauth2_header)

    # Return authorized http transport
    return oauth2_header
