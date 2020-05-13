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
  --activate-apis   Activate all missing but required Cloud APIs
  --create-service-account
                    Create the service account and client secrets

  --deploy-repo     (Re)Deploy the code from a GCP source repository named 'report2bq'
                    in the current project.

  --deploy-all      (Re)Deploy all portions
  OR
  --deploy-bigquery (Re)Deploy Big Query dataset
  --deploy-fetcher  (Re)Deploy fetcher
  --deploy-job-monitor
                    (Re)Deploy job monitor
  --deploy-loader   (Re)Deploy loader
  --deploy-runner   (Re)Deploy report runners and monitor
  --deploy-storage  (Re)Deploy GCS buckets
  --deploy-trigger  (Re)Deploy triggers
  --deploy-oauth    (Re)Deploy OAuth generator
  --deploy-job-manager
                    (Re)Deploy the job manager for listing creating and deleting scheduled jobs

  --background      Run the Cloud Functions deployments in the background.
                    WARNING: IF YOU DO THIS, BE SURE TO CHECK THE FUNCTION OUTPUT FILES FOR A
                             SUCCESSFUL COMPLETION.
  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful 
                    for testing.
  --usage           Show this text
EOF
}

# Switch definitions
PROJECT=
USER=
DATASET="report2bq"

ACTIVATE_APIS=0
BACKGROUND=0
CREATE_SERVICE_ACCOUNT=0
DEPLOY_BQ=0
DEPLOY_CODE=1
DEPLOY_FETCHER=0
DEPLOY_JOB_MANAGER=0
DEPLOY_LOADER=0
DEPLOY_MONITOR=0
DEPLOY_OAUTH=0
DEPLOY_RUNNERS=0
DEPLOY_STORAGE=0
DEPLOY_TRIGGER=0
USERNAME=0

# Command line parser
while [[ $1 == -* ]] ; do
  case $1 in 
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --dataset*)
      IFS="=" read _cmd DATASET <<< "$1" && [ -z ${DATASET} ] && shift && DATASET=$1
      ;;
    --deploy-all)
      DEPLOY_BQ=1
      DEPLOY_FETCHER=1
      DEPLOY_JOB_MANAGER=1
      DEPLOY_LOADER=1
      DEPLOY_MONITOR=1
      DEPLOY_OAUTH=1
      DEPLOY_RUNNERS=1
      DEPLOY_STORAGE=1
      DEPLOY_TRIGGER=1
      ;;
    --deploy-bigquery)
      DEPLOY_BQ=1
      ;;  
    --deploy-oauth)
      DEPLOY_OAUTH=1
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
    --deploy-storage)
      DEPLOY_STORAGE=1
      ;;
    --deploy-repo)
      DEPLOY_CODE=0
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
    --dry-run)
      DRY_RUN=echo
      ;;
    *)
      usage
      echo ""
      echo "Unknown parameter $1."
      exit
  esac
  shift
done


if [ "x${PROJECT}" == "x" ]; then
  usage
  echo ""
  echo You must specify a project.
  exit
fi

USER=report2bq@${PROJECT}.iam.gserviceaccount.com

if [ ${ACTIVATE_APIS} -eq 1 ]; then
  # Check for active APIs
  APIS_USED=(
    "adsdatahub"
    "appengine"
    "bigquery"
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
  ${DRY_RUN} gcloud projects add-iam-policy-binding ${PROJECT} --member=serviceAccount:${USER} --role=roles/cloudfunctions.invoker
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
  ${DRY_RUN} zip report2bq.zip main.py requirements.txt README.md LICENSE CONTRIBUTING.md classes/*.py cloud_functions/*.py screenshots/*.*

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

# Deploy uploader trigger
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

# Deploy job monitor
if [ ${DEPLOY_MONITOR} -eq 1 ]; then
  if [ ${BACKGROUND} -eq 1 ]; then
    _BG=" & > report2bq-monitor.deploy 2>&1"
  fi

  # Create topic
  ${DRY_RUN} gcloud pubsub topics delete \
    --project=${PROJECT} \
    --quiet \
    "job-monitor"

  ${DRY_RUN} gcloud pubsub topics create \
    --project=${PROJECT} \
    --quiet \
    "job-monitor"

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
    --set-env-vars=BQ_DATASET=${DATASET} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}
fi

# Deploy runners
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
    --quiet \
    --timeout=540s \
    --project=${PROJECT} ${_BG}

  # Create topic
  ${DRY_RUN} gcloud pubsub topics delete \
    --project=${PROJECT} \
    --quiet \
    "run-monitor"

  ${DRY_RUN} gcloud pubsub topics create \
    --project=${PROJECT} \
    --quiet \
    "run-monitor"

  # Deploy cloud function
  echo "run-monitor"
  ${DRY_RUN} gcloud functions deploy "run-monitor" \
    --service-account=$USER \
    --entry-point=run_monitor \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-topic="run-monitor" \
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
    --schedule="2/5 * * * *" \
    --topic="projects/${PROJECT}/topics/run-monitor" \
    --time-zone="America/Toronto" \
    --attributes="project=${PROJECT}" \
    --message-body="RUN" \
    --project=${PROJECT}
fi

# Deploy oauth
if [ ${DEPLOY_OAUTH} -eq 1 ]; then
  if [ ${BACKGROUND} -eq 1 ]; then
    _BG=" & > report2bq-oauth.deploy 2>&1"
  fi

  ${DRY_RUN} gcloud functions deploy "OAuthRequest" \
    --service-account=$USER \
    --entry-point=oauth_request \
    --allow-unauthenticated \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-http \
    --ingress-settings=internal-only \
    --quiet \
    --timeout=60s \
    --project=${PROJECT} ${_BG}

  echo "OAuth"
  ${DRY_RUN} gcloud functions deploy "OAuth" \
    --service-account=$USER \
    --entry-point=oauth \
    --allow-unauthenticated \
    --source=${SOURCE} \
    --ingress-settings=internal-only \
    --runtime python37 \
    --memory=1024MB \
    --trigger-http \
    --quiet \
    --timeout=60s \
    --project=${PROJECT} ${_BG}

  echo "OAuthComplete"
  ${DRY_RUN} gcloud functions deploy "OAuthComplete" \
    --service-account=$USER \
    --entry-point=oauth_complete \
    --allow-unauthenticated \
    --source=${SOURCE} \
    --ingress-settings=internal-only \
    --runtime python37 \
    --memory=1024MB \
    --trigger-http \
    --quiet \
    --timeout=60s \
    --project=${PROJECT} ${_BG}
fi
