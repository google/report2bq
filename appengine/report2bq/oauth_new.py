from apiclient import discovery
import httplib2
import logging
from oauth2client import client
from google.cloud import storage
import json
from google.oauth2 import id_token
from google.auth.transport import requests

# (Receive auth_code by HTTPS POST)
request = requests.Request()
logging.info(request)

# If this request does not have `X-Requested-With` header, this could be a CSRF
# if not request.headers.get('X-Requested-With'):
#     abort(403)

SCOPES = [
  'https://www.googleapis.com/auth/adsdatahub', # ADH
  'https://www.googleapis.com/auth/bigquery', # BigQuery
  'https://www.googleapis.com/auth/cloud-platform', # Cloud Platform
  'https://www.googleapis.com/auth/datastore', # Firestore
  'https://www.googleapis.com/auth/dfareporting', # DCM Reporting
  'https://www.googleapis.com/auth/devstorage.read_write', # GCS
  'https://www.googleapis.com/auth/doubleclickbidmanager', # DBM
  'https://www.googleapis.com/auth/doubleclicksearch', # Firestore
]

storage_client = storage.Client()
logging.info(client._credentials)

# Set path to the Web application client_secret_*.json file you downloaded from the
# Google API Console: https://console.developers.google.com/apis/credentials
CLIENT_SECRET_FILE = '/path/to/client_secret.json'
CLIENT_SECRETS = json.loads(
    storage_client.get_bucket(
            'galvanic-card-234919-report2bq-tokens'
    ).blob('client_secrets.json').blob.download_as_string()
)

# Exchange auth code for access token, refresh token, and ID token
# credentials = client.credentials_from_clientsecrets_and_code(
#     CLIENT_SECRET_FILE,
#     ['https://www.googleapis.com/auth/drive.appdata', 'profile', 'email'],
#     auth_code)
credentials = client.credentials_from_code(
  client_id=CLIENT_SECRETS['client_id'],
  client_secret=CLIENT_SECRETS['client_secret'],
  scope=SCOPES,
  code=auth_code
)

logging.info(credentials)

# Call Google API
# http_auth = credentials.authorize(httplib2.Http())
# drive_service = discovery.build('drive', 'v3', http=http_auth)
# appfolder = drive_service.files().get(fileId='appfolder').execute()

# Get profile info from ID token
userid = credentials.id_token['sub']
email = credentials.id_token['email']