#!/bin/bash
# Copyright 2020 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# install.sh
#
# Install all the base libraries, APIs, datasets and Cloud Functions for Repor2BQ to
# run

# Functions
function usage() {
  cat << EOF
install.sh
==========

Usage:
  install.sh [options]

Options:
  --project         GCP Project Id
  --dataset         The Big Query dataset to verify or create
  --api-key         The API key

Deployment directives:
  --activate-apis   Activate all missing but required Cloud APIs
  --create-service-account
                    Create the service account and client secrets
  OR
  --service-account The service account to use to run the cloud functions

  EITHER:
    --deploy-all      (Re)Deploy all portions

  OR ONE OR MORE OF:
    --deploy-bigquery (Re)Deploy Big Query dataset
    --deploy-fetcher  (Re)Deploy fetcher
    --deploy-job-monitor
                      (Re)Deploy job monitor
    --deploy-loader   (Re)Deploy loader
    --deploy-runner   (Re)Deploy report runners
    --deploy-run-monitor
                      (Re)Deploy run monitor
    --deploy-storage  (Re)Deploy GCS buckets
    --deploy-trigger  (Re)Deploy triggers
    --deploy-job-manager
                      (Re)Deploy the job manager for listing creating and deleting scheduled jobs
    --deploy-ga360-manager
                      (Re)Deploy the report manager for dynamic GA360 reports
    --deploy-sa360-manager
                      (Re)Deploy the report manager for dynamic SA360 reports

  --no-topics       Just deploy the functions; good if you have deployed once and are just
                    updating the cloud functions

General switches:
  --administrator   EMail address of the administrator for error messages
  --store-api-key   Store the API key in the Secret Manager for use later

  --store-client
  --client-secret
  --client-id

  --no-[ adh | cm | dv360 | ga360 | sa360 ]
                    These keys will not enable access to the named product.
                    If you choose to do this, you will still be able to
                    create runners and fetchers for the product, but Report2BQ
                    will not be able to execute them. Any combination of these
                    can be used. For example: --no-adh --no-ga360  will not
                    enable ADH or GA360, but will enable CM, DV360 and SA360
                    for Report2BQ to use.
                    If the Google Cloud Project already has the APIs enabled,
                    that status will NOT change, regardless of the switch.
                    The default is that all will be enabled.

  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful
                    for testing.
  --usage           Show this text
EOF
}

function join { local IFS="$1"; shift; echo "$*"; }

function check_service {
  _SERVICE=$1
  [[ "${ACTIVE_SERVICES}" =~ ${_SERVICE} ]]
}

# Constants
PYTHON_RUNTIME=python310

# Switch definitions
PROJECT=
USER=
DATASET="report2bq"
API_KEY=

ACTIVATE_APIS=0
CREATE_SERVICE_ACCOUNT=0
DEPLOY_BQ=0
DEPLOY_CODE=1
DEPLOY_FETCHER=0
DEPLOY_JOB_MANAGER=0
DEPLOY_LOADER=0
DEPLOY_MONITOR=0
DEPLOY_RUNNERS=0
DEPLOY_RUN_MONITOR=0
DEPLOY_STORAGE=0
DEPLOY_TRIGGER=0
DEPLOY_POSTPROCESSOR=0
DEPLOY_GA360_MANAGER=0
DEPLOY_SA360_MANAGER=0
DEPLOY_TOPICS=1
STORE_API_KEY=0
STORE_CLIENT=0

ADH=1
CM=1
DV360=1
GA360=1
SA360=1

USERNAME=0
ADMIN=
CLIENT_ID=
CLIENT_SECRET=

# Command line parser
while [[ $1 == -* ]] ; do
  case $1 in
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --dataset*)
      IFS="=" read _cmd DATASET <<< "$1" && [ -z ${DATASET} ] && shift && DATASET=$1
      ;;
    --api-key*)
      IFS="=" read _cmd API_KEY <<< "$1" && [ -z ${API_KEY} ] && shift && API_KEY=$1
      ;;
    --administrator*)
      IFS="=" read _cmd ADMIN <<< "$1" && [ -z ${ADMIN} ] && shift && ADMIN=$1
      ;;
    --deploy-all)
      DEPLOY_BQ=1
      DEPLOY_FETCHER=1
      DEPLOY_JOB_MANAGER=1
      DEPLOY_LOADER=1
      DEPLOY_MONITOR=1
      DEPLOY_RUNNERS=1
      DEPLOY_RUN_MONITOR=1
      DEPLOY_STORAGE=1
      DEPLOY_TRIGGER=1
      DEPLOY_POSTPROCESSOR=1
      DEPLOY_GA360_MANAGER=1
      DEPLOY_SA360_MANAGER=1
      ;;
    --deploy-bigquery)
      DEPLOY_BQ=1
      ;;
    --deploy-fetcher)
      DEPLOY_FETCHER=1
      ;;
    --deploy-loader)
      DEPLOY_LOADER=1
      ;;
    --deploy-trigger)
      DEPLOY_TRIGGER=1
      ;;
    --deploy-job-monitor)
      DEPLOY_MONITOR=1
      ;;
    --deploy-job-manager)
      DEPLOY_JOB_MANAGER=1
      ;;
    --deploy-runner)
      DEPLOY_RUNNERS=1
      ;;
    --deploy-run-monitor)
      DEPLOY_RUN_MONITOR=1
      ;;
    --deploy-storage)
      DEPLOY_STORAGE=1
      ;;
    --deploy-postprocessor)
      DEPLOY_POSTPROCESSOR=1
      ;;
    --deploy-ga360-manager)
      DEPLOY_GA360_MANAGER=1
      ;;
    --deploy-sa360-manager)
      DEPLOY_SA360_MANAGER=1
      ;;
    --activate-apis)
      ACTIVATE_APIS=1
      ;;
    --create-service-account)
      CREATE_SERVICE_ACCOUNT=1
      ;;
    --service-account*)
      IFS="=" read _cmd USER <<< "$1" && [ -z ${USER} ] && shift && USER=$1
      ;;
    --store-api-key)
      STORE_API_KEY=1
      ;;
    --dry-run)
      DRY_RUN=echo
      ;;
    --no-topics)
      DEPLOY_TOPICS=0
      ;;
    --no-code)
      DEPLOY_CODE=0
      ;;
    --no-adh)
      ADH=0
      ;;
    --no-cm)
      CM=0
      ;;
    --no-dv360)
      DV360=0
      ;;
    --no-ga360)
      GA360=0
      ;;
    --no-sa360)
      SA360=0
      ;;
    --store-client)
      STORE_CLIENT=1
      ;;
    --client-id*)
      IFS="=" read _cmd CLIENT_ID <<< "$1" && [ -z ${CLIENT_ID} ] && shift && CLIENT_ID=$1
      ;;
    --client-secret*)
      IFS="=" read _cmd CLIENT_SECRET <<< "$1" && [ -z ${CLIENT_SECRET} ] && shift && CLIENT_SECRET=$1
      ;;
    *)
      usage
      echo ""
      echo "Unknown parameter $1."
      exit
  esac
  shift
done

if [ -z "${API_KEY}" ]; then
  read API_KEY <<< $(gcloud --project ${PROJECT} secrets versions access latest --secret=api_key 2>/dev/null)
fi

if [ -z "${PROJECT}" -o -z "${API_KEY}" ]; then
  usage
  echo ""
  echo You must specify a project and API key to proceed.
  exit
fi

if [ ! -z ${ADMIN} ]; then
  _ADMIN="ADMINISTRATOR_EMAIL=${ADMIN}"
fi

if [ -z "${USER}" -a ${CREATE_SERVICE_ACCOUNT} -eq "0" ]; then
  read USER <<< $(gsutil cat gs://${PROJECT}-report2bq-tokens/service_account 2>/dev/null)
fi

if [   "${USER}" -a ${CREATE_SERVICE_ACCOUNT} -eq 1 \
    -o -z "${USER}" -a ${CREATE_SERVICE_ACCOUNT} -eq 0 ]; then
  echo "Please specify one and only one of --create-service-account and --service-account"
fi

ACTIVE_SERVICES="$(gcloud --project=${PROJECT} services list | cut -f 1 -d' ' | grep -v NAME)"

# Check for active APIs
if [ ${ACTIVATE_APIS} -eq 1 ]; then
  # Support APIs - all these are required
  APIS_USED=(
    "appengine"
    "bigquery"
    "cloudbuild"
    "cloudfunctions"
    "cloudscheduler"
    "firestore"
    "gmail"
    "pubsub"
    "secretmanager"
    "serviceusage"
    "storage-api"
  )

  (( ADH )) && APIS_USED+=("adsdatahub")
  (( GA360 )) && APIS_USED+=("analyticsreporting")
  (( CM )) && APIS_USED+=("dfareporting")
  (( DV360 )) && APIS_USED+=("doubleclickbidmanager")
  (( SA360 )) && APIS_USED+=("doubleclicksearch")

  for api in ${APIS_USED[@]}; do
    if check_service ${api}; then
      echo "${api} already active"
    else
      echo "Activating ${api}"
      ${DRY_RUN} gcloud --project=${PROJECT} services enable ${api}.googleapis.com
    fi
  done
fi

if [ ${DEPLOY_BQ} -eq 1 ]; then
  # Create datasets
    for _dataset in ${DATASET} report2bq_admin; do
    bq --project_id=${PROJECT} show --dataset ${_dataset} > /dev/null 2>&1
    RETVAL=$?
    if (( $RETVAL != "0" )); then
      ${DRY_RUN} bq --project_id=${PROJECT} mk --dataset ${_dataset}
    fi
  done
fi

if [ ${DEPLOY_STORAGE} -eq 1 ]; then
  # Create buckets
  for bucket in report2bq report2bq-tokens report2bq-upload report2bq-postprocessor report2bq-sa360-manager report2bq-ga360-manager; do
    gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket} > /dev/null 2>&1
    RETVAL=$?
    if (( ${RETVAL} != "0" )); then
      ${DRY_RUN} gsutil mb -p ${PROJECT} gs://${PROJECT}-${bucket}
    fi
  done
fi

if [ ${CREATE_SERVICE_ACCOUNT} -eq 1 ]; then
  USER=report2bq@${PROJECT}.iam.gserviceaccount.com
  ${DRY_RUN} gcloud --project ${PROJECT} iam service-accounts create report2bq --description "Report2BQ Service Account" \
  && ${DRY_RUN} gcloud --project ${PROJECT} iam service-accounts keys create "report2bq@${PROJECT}.iam.gserviceaccount.com.json" --iam-account ${USER}
  ${DRY_RUN} gcloud projects add-iam-policy-binding ${PROJECT} --member=serviceAccount:${USER} --role=roles/editor
fi

echo ${USER} | gsutil cp - gs://${PROJECT}-report2bq-tokens/service_account

if [ ${STORE_API_KEY} -eq 1 ]; then
  gcloud --project ${PROJECT} secrets create api_key --replication-policy=automatic 2>/dev/null
  echo ${API_KEY} | gcloud --project ${PROJECT} secrets versions add api_key --data-file=-
fi

if [ ${STORE_CLIENT} -eq 1 ]; then
  if [ -z ${CLIENT_ID} ] || [ -z ${CLIENT_SECRET} ]; then
    echo To store the client details you must supply CLIENT_ID and CLIENT_SECRET.
    exit
  else
    ${DRY_RUN} gcloud --project ${PROJECT} secrets create client_id --replication-policy=automatic 2>/dev/null
    echo "{ \"client_id\": \"${CLIENT_ID}\" }" | ${DRY_RUN} gcloud --project ${PROJECT} secrets versions add client_id --data-file=-

    ${DRY_RUN} gcloud --project ${PROJECT} secrets create client_secret --replication-policy=automatic 2>/dev/null
    echo "{ \"client_id\": \"${CLIENT_SECRET}\" }" | ${DRY_RUN} gcloud --project ${PROJECT} secrets versions add client_secret --data-file=-
  fi
fi

if [ ${DEPLOY_CODE} -eq 1 ]; then
  # Create and deploy the code
  # Check for the code bucket
  ${DRY_RUN} gsutil ls -p ${PROJECT} gs://${PROJECT}-report2bq > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo Destination bucket missing. Creating.
    ${DRY_RUN} gsutil mb -p ${PROJECT} gs://${PROJECT}-report2bq
  fi

  [ -e report2bq.zip ] && rm -f report2bq.zip >/dev/null 2>&1

  # Create the zip
  ${DRY_RUN}              \
    zip report2bq.zip     \
      -r main.py          \
      requirements.txt    \
      classes/            \
      cloud_functions/    \
      -x *_test.py        \
      -x __pycache__      \
      -x *.pyc

  # Copy it up
  ${DRY_RUN} gsutil cp report2bq.zip gs://${PROJECT}-report2bq > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo Error deploying zip file!
    exit
  fi
  SOURCE="gs://${PROJECT}-report2bq/report2bq.zip"
  ${DRY_RUN} gsutil cp postprocessors/report2bq_unknown.py \
    gs://${PROJECT}-report2bq-postprocessor 2>&1 > /dev/null

else
  SOURCE="."
fi

# TOPICS & TRIGGERS
if [ ${DEPLOY_TOPICS} -eq 1 ]; then
  # fetcher
  if [ ${DEPLOY_TRIGGER} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-fetcher"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-fetcher"
  fi

  # job-monitor
  if [ ${DEPLOY_MONITOR} -eq 1 ]; then
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-job-monitor"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-job-monitor"
  fi

  # report-runner
  if [ ${DEPLOY_RUNNERS} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-runner"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-runner"
  fi

  # run-monitor
  if [ ${DEPLOY_RUN_MONITOR} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-run-monitor"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-run-monitor"
  fi

  # post-processor
  if [ ${DEPLOY_POSTPROCESSOR} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-postprocessor"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-postprocessor"
  fi

  if [ ${DEPLOY_SA360_MANAGER} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-bq-creator"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-bq-creator"
  fi

  if [ ${DEPLOY_JOB_MANAGER} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report2bq-job-manager"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-job-manager"
  fi
fi

# CLOUD FUNCTIONS
_ENV_VARS=(
  "DATASET=${DATASET}"
  "API_KEY=${API_KEY}"
  "GCP_PROJECT=${PROJECT}"
  "POSTPROCESSOR=report2bq-postprocessor"
  ${_ADMIN}
)

for api in adsdatahub analyticsreporting dfareporting doubleclickbidmanager doubleclicksearch; do
  if ! check_service ${api}; then
    case ${api} in
      adsdatahub)
        _ENV_VARS+=("ADH=False")
        ;;
      analyticsreporting)
        _ENV_VARS+=("GA360=False")
        ;;
      dfareporting)
        _ENV_VARS+=("CM=False")
        ;;
      doubleclickbidmanager)
        _ENV_VARS+=("DV360=False")
        ;;
      doubleclicksearch)
        _ENV_VARS+=("SA360=False")
        ;;
    esac
  fi
done
ENVIRONMENT=$(join "," ${_ENV_VARS[@]})

# Deploy job monitor
if [ ${DEPLOY_MONITOR} -eq 1 ]; then
  # Deploy cloud function
  echo "job-monitor"
  ${DRY_RUN} gcloud functions deploy "report2bq-job-monitor" \
    --entry-point=job_monitor \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=1024MB \
    --trigger-topic="report2bq-job-monitor" \
    --set-env-vars=${ENVIRONMENT} \
    --service-account=${USER} \
    --quiet \
    --timeout=240s \
    --max-instances=1 \
    --project=${PROJECT} ${_BG}

  # Create scheduled job
  ${DRY_RUN} gcloud beta scheduler jobs delete \
    --project=${PROJECT} \
    --quiet \
    "report2bq-job-monitor"

  ${DRY_RUN} gcloud beta scheduler jobs create pubsub \
    "report2bq-job-monitor" \
    --schedule="*/2 * * * *" \
    --topic="projects/${PROJECT}/topics/report2bq-job-monitor" \
    --time-zone="America/Toronto" \
    --message-body="RUN" \
    --project=${PROJECT} ${_BG}
fi

# Fetcher
if [ ${DEPLOY_FETCHER} -eq 1 ]; then
  # Deploy cloud function
  echo "report2bq-fetcher"
  ${DRY_RUN} gcloud functions deploy "report2bq-fetcher" \
    --entry-point=report_fetch \
    --source=${SOURCE} \
    --runtime=${PYTHON_RUNTIME} \
    --memory=8192MB \
    --trigger-topic="report2bq-fetcher" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

# Loader
if [ ${DEPLOY_LOADER} -eq 1 ]; then
  # Deploy cloud function
  echo "report2bq-loader"
  ${DRY_RUN} gcloud functions deploy "report2bq-loader" \
    --entry-point=report_upload \
    --source=${SOURCE} \
    --runtime=${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-resource="projects/_/buckets/${PROJECT}-report2bq-upload" \
    --trigger-event="google.storage.object.finalize" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

# Deploy runners
if [ ${DEPLOY_RUNNERS} -eq 1 ]; then
  # Deploy cloud function
  echo "report-runner"

  ${DRY_RUN} gcloud functions deploy "report2bq-runner" \
    --entry-point=report_runner \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=2048MB \
    --trigger-topic="report2bq-runner" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

if [ ${DEPLOY_RUN_MONITOR} -eq 1 ]; then
  # Deploy cloud function
  echo "run-monitor"

  ${DRY_RUN} gcloud functions deploy "report2bq-run-monitor" \
    --entry-point=run_monitor \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=1024MB \
    --trigger-topic="report2bq-run-monitor" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --max-instances=1 \
    --project=${PROJECT} ${_BG}

  # Create scheduled job
  ${DRY_RUN} gcloud beta scheduler jobs delete \
    --project=${PROJECT} \
    --quiet \
    "report2bq-run-monitor"

  ${DRY_RUN} gcloud beta scheduler jobs create pubsub "report2bq-run-monitor" \
    --schedule="1-59/2 * * * *" \
    --topic="projects/${PROJECT}/topics/report2bq-run-monitor" \
    --time-zone="America/Toronto" \
    --message-body="RUN" \
    --project=${PROJECT}
fi

if [ ${DEPLOY_POSTPROCESSOR} -eq 1 ]; then
  # Deploy cloud function
  echo "postprocessor"
  ${DRY_RUN} gcloud functions deploy "report2bq-postprocessor" \
    --entry-point=post_processor \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-topic="report2bq-postprocessor" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --project=${PROJECT} ${_BG}
fi

if [ ${DEPLOY_SA360_MANAGER} -eq 1 ]; then
  # Deploy cloud function
  echo "sa360 manager"
  ${DRY_RUN} gcloud functions deploy "report2bq-sa360-manager" \
    --entry-point=report_manager \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-resource="projects/_/buckets/${PROJECT}-report2bq-sa360-manager" \
    --trigger-event="google.storage.object.finalize" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --project=${PROJECT} ${_BG}

  ${DRY_RUN} gcloud functions deploy "report2bq-bq-sa360-report-creator" \
    --entry-point=sa360_report_creator \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-topic="report2bq-bq-creator" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --project=${PROJECT} ${_BG}

  parameters=(
    "--project=report2bq-zz9-plural-z-alpha",
    "--email=davidharcombe@google.com",
  )
  ${DRY_RUN} gcloud beta scheduler jobs create pubsub "report2bq-bq-sa360-report-creator" \
    --schedule="50 * * * *" \
    --topic="projects/${PROJECT}/topics/report2bq-bq-creator" \
    --time-zone="America/Toronto" \
    --message-body="RUN" \
    --attributes="project=${PROJECT},email=davidharcombe@google.com" \
    --project=${PROJECT}
fi

if [ ${DEPLOY_GA360_MANAGER} -eq 1 ]; then
  # Deploy cloud function
  echo "ga360 manager"
  ${DRY_RUN} gcloud functions deploy "report2bq-ga360-manager" \
    --entry-point=report_manager \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-resource="projects/_/buckets/${PROJECT}-report2bq-ga360-manager" \
    --trigger-event="google.storage.object.finalize" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --project=${PROJECT} ${_BG}
fi

if [ ${DEPLOY_JOB_MANAGER} -eq 1 ]; then
  # Deploy cloud function
  echo "job manager (async)"
  ${DRY_RUN} gcloud functions deploy "report2bq-job-manager-pubsub" \
    --entry-point=job_manager \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-topic="report2bq-job-manager" \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --project=${PROJECT} ${_BG}

  echo "job manager (async)"
  ${DRY_RUN} gcloud functions deploy "report2bq-job-manager-http" \
    --entry-point=job_manager_http \
    --source=${SOURCE} \
    --runtime ${PYTHON_RUNTIME} \
    --memory=4096MB \
    --trigger-http \
    --allow-unauthenticated \
    --service-account=${USER} \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=240s \
    --project=${PROJECT} ${_BG}
fi
