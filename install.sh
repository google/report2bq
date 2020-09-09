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

# __author__ = [
#   'davidharcombe@google.com (David Harcombe)'
# ]

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
  --dataset         The Big Query datase to verify or create
  --api-key         The API key

Deployment directives:
  --activate-apis   Activate all missing but required Cloud APIs
  --create-service-account
                    Create the service account and client secrets
  
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

  --no-topics       Just deploy the functions; good if you have deployed once and are just 
                    updating the cloud functions
  --background      Run the Cloud Functions deployments in the background.
                    WARNING: IF YOU DO THIS, BE SURE TO CHECK THE FUNCTION OUTPUT FILES FOR A
                             SUCCESSFUL COMPLETION.

General switches:
  --administrator   EMail address of the administrator for error messages
  --store-api-key   Store the API key in the GCS tokens bucket for use later
  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful 
                    for testing.
  --usage           Show this text
EOF
}

function join { local IFS="$1"; shift; echo "$*"; }

# Switch definitions
PROJECT=
USER=
DATASET="report2bq"
API_KEY=

ACTIVATE_APIS=0
BACKGROUND=0
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
DEPLOY_TOPICS=1
STORE_API_KEY=0
USERNAME=0
ADMIN=

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
    --activate-apis)
      ACTIVATE_APIS=1
      ;;
    --create-service-account)
      CREATE_SERVICE_ACCOUNT=1
      ;;
    --background)
      BACKGROUND=1
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
    *)
      usage
      echo ""
      echo "Unknown parameter $1."
      exit
  esac
  shift
done

if [ -z ${API_KEY} ]; then
  read API_KEY <<< $(gsutil cat gs://${PROJECT}-report2bq-tokens/api.key 2>/dev/null)
fi

if [ -z ${PROJECT} -o -z ${API_KEY} ]; then
  usage
  echo ""
  echo You must specify a project and API key to proceed.
  exit
fi

USER=report2bq@${PROJECT}.iam.gserviceaccount.com
if [ ! -z ${ADMIN} ]; then
  _ADMIN="ADMINISTRATOR_EMAIL=${ADMIN}"
fi

if [ ${ACTIVATE_APIS} -eq 1 ]; then
  # Check for active APIs
  APIS_USED=(
    "adsdatahub"
    "appengine"
    "bigquery"
    "cloudbuild"
    "cloudfunctions"
    "cloudscheduler"
    "dfareporting"
    "doubleclickbidmanager"
    "doubleclicksearch"
    "firestore"
    "pubsub"
    "storage-api"
  )
  ACTIVE_SERVICES="$(gcloud --project=${PROJECT} services list | cut -f 1 -d' ' | grep -v NAME)"
  # echo ${ACTIVE_SERVICES[@]}

  for api in ${APIS_USED[@]}; do
    if [[ "${ACTIVE_SERVICES}" =~ ${api} ]]; then
      echo "${api} already active"
    else
      echo "Activating ${api}"
      ${DRY_RUN} gcloud --project=${PROJECT} services enable ${api}.googleapis.com
    fi
  done
fi

if [ ${DEPLOY_BQ} -eq 1 ]; then
  # Create dataset
  bq --project_id=${PROJECT} show --dataset ${DATASET} > /dev/null 2>&1
  RETVAL=$?
  if (( $RETVAL != "0" )); then
    ${DRY_RUN} bq --project_id=${PROJECT} mk --dataset ${DATASET}
  fi
fi

if [ ${DEPLOY_STORAGE} -eq 1 ]; then
  # Create buckets
  for bucket in report2bq-tokens report2bq-upload report2bq; do
    gsutil ls -p ${PROJECT} gs://${PROJECT}-${bucket} > /dev/null 2>&1
    RETVAL=$?
    if (( ${RETVAL} != "0" )); then
      ${DRY_RUN} gsutil mb -p ${PROJECT} gs://${PROJECT}-${bucket}
    fi
  done
fi

if [ ${CREATE_SERVICE_ACCOUNT} -eq 1 ]; then
  ${DRY_RUN} gcloud iam service-accounts create report2bq --description "Report2BQ Service Account" --project ${PROJECT} \
  && ${DRY_RUN} gcloud iam service-accounts keys create "report2bq@${PROJECT}.iam.gserviceaccount.com.json" --iam-account ${USER} --project ${PROJECT} \
  && ${DRY_RUN} gsutil cp "report2bq@${PROJECT}.iam.gserviceaccount.com.json" gs://${PROJECT}-report2bq-tokens/
  ${DRY_RUN} gcloud projects add-iam-policy-binding ${PROJECT} --member=serviceAccount:${USER} --role=roles/editor
fi

if [ ${STORE_API_KEY} -eq 1 ]; then
  echo ${API_KEY} | gsutil cp - gs://${PROJECT}-report2bq-tokens/api.key
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
  ${DRY_RUN} zip report2bq.zip -r main.py requirements.txt README.md LICENSE CONTRIBUTING.md classes/ cloud_functions/ screenshots/ -x __pycache__ -x *.pyc

  # Copy it up
  ${DRY_RUN} gsutil cp report2bq.zip gs://${PROJECT}-report2bq > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo Error deploying zip file!
    exit
  fi
  SOURCE="gs://${PROJECT}-report2bq/report2bq.zip"
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
      "report2bq-trigger"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report2bq-trigger"
  fi

  # job-monitor
  if [ ${DEPLOY_MONITOR} -eq 1 ]; then
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "job-monitor"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "job-monitor"
  fi

  # report-runner
  if [ ${DEPLOY_RUNNERS} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "report-runner"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "report-runner"
  fi

  # run-monitor
  if [ ${DEPLOY_RUN_MONITOR} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "run-monitor"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "run-monitor"
  fi

  # post-processor
  if [ ${DEPLOY_POSTPROCESSOR} -eq 1 ]; then
    # Create topic
    ${DRY_RUN} gcloud pubsub topics delete \
      --project=${PROJECT} \
      --quiet \
      "postprocessor"

    ${DRY_RUN} gcloud pubsub topics create \
      --project=${PROJECT} \
      --quiet \
      "postprocessor"
  fi

fi

# CLOUD FUNCTIONS
_ENV_VARS=(
  "DATASET=${DATASET}"
  "API_KEY=${API_KEY}"
  ${_ADMIN}
)
ENVIRONMENT=$(join "," ${_ENV_VARS[@]})

# Deploy job monitor
if [ ${DEPLOY_MONITOR} -eq 1 ]; then
  if [ ${BACKGROUND} -eq 1 ]; then
    _BG=" & > report2bq-monitor.deploy 2>&1"
  fi

  # Deploy cloud function 
  echo "job-monitor"
  ${DRY_RUN} gcloud functions deploy "job-monitor" \
    --entry-point=job_monitor \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-topic="job-monitor" \
    --service-account=$USER \
    --quiet \
    --timeout=60s \
    --project=${PROJECT} ${_BG}

  # Create scheduled job
  ${DRY_RUN} gcloud beta scheduler jobs delete \
    --project=${PROJECT} \
    --quiet \
    "job-monitor"

  ${DRY_RUN} gcloud beta scheduler jobs create pubsub \
    "job-monitor" \
    --schedule="*/5 * * * *" \
    --topic="projects/${PROJECT}/topics/job-monitor" \
    --time-zone="America/Toronto" \
    --message-body="RUN" \
    --project=${PROJECT} ${_BG}
fi

# Fetcher
if [ ${DEPLOY_FETCHER} -eq 1 ]; then
  # Deploy cloud function
  if [ ${BACKGROUND} -eq 1 ]; then
    _BG=" & > report2bq-fetcher.deploy 2>&1"
  fi

  echo "report2bq-fetcher"
  ${DRY_RUN} gcloud functions deploy "report2bq-fetcher" \
    --entry-point=report_fetch \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-topic="report2bq-trigger" \
    --service-account=$USER \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

# Loader
if [ ${DEPLOY_LOADER} -eq 1 ]; then
  # Deploy cloud function
  if [ ${BACKGROUND} -eq 1 ]; then
    _BG=" & > report2bq-loader.deploy 2>&1"
  fi

  echo "report2bq-loader"
  ${DRY_RUN} gcloud functions deploy "report2bq-loader" \
    --entry-point=report_upload \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-resource="projects/_/buckets/${PROJECT}-report2bq-upload" \
    --trigger-event="google.storage.object.finalize" \
    --service-account=$USER \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

# Deploy runners
if [ ${DEPLOY_RUNNERS} -eq 1 ]; then
  # Deploy cloud function
  echo "report-runner"
  if [ ${BACKGROUND} -eq 1 ]; then
    _BG=" & > report2bq-runner.deploy 2>&1"
  fi

  ${DRY_RUN} gcloud functions deploy "report-runner" \
    --entry-point=report_runner \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-topic="report-runner" \
    --service-account=$USER \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

if [ ${DEPLOY_RUN_MONITOR} -eq 1 ]; then
  # Deploy cloud function
  echo "run-monitor"
  ${DRY_RUN} gcloud functions deploy "run-monitor" \
    --service-account=$USER \
    --entry-point=run_monitor \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-topic="run-monitor" \
    --service-account=$USER \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=60s \
    --project=${PROJECT} ${_BG}

  # Create scheduled job
  ${DRY_RUN} gcloud beta scheduler jobs delete \
    --project=${PROJECT} \
    --quiet \
    "run-monitor"

  ${DRY_RUN} gcloud beta scheduler jobs create pubsub \
    "run-monitor" \
    --schedule="*/5 * * * *" \
    --topic="projects/${PROJECT}/topics/run-monitor" \
    --time-zone="America/Toronto" \
    --message-body="RUN" \
    --project=${PROJECT}
fi

if [ ${DEPLOY_POSTPROCESSOR} -eq 1 ]; then
  # Deploy cloud function
  echo "postprocessor"
  ${DRY_RUN} gcloud functions deploy "postprocessor" \
    --service-account=$USER \
    --entry-point=post_processor \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-topic="postprocessor" \
    --service-account=$USER \
    --set-env-vars=${ENVIRONMENT} \
    --quiet \
    --timeout=60s \
    --project=${PROJECT} ${_BG}
fi
