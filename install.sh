#!/bin/bash
# Copyright 2017 Google Inc. All Rights Reserved.
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

  --deploy-code     (Re)Deploy the zip file of code. If this is not specified,
                    the cloud functions will be deployed from a Cloud Repository names 'report2bq'
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

  --dry-run         Don't do anything, just print the commands you would otherwise run. Useful 
                    for testing.
  --usage           Show this text
EOF
}

# Switch definitions
PROJECT=
DATASET="report2bq"
DEPLOY_FETCHER=0
DEPLOY_LOADER=0
DEPLOY_TRIGGER=0
DEPLOY_MONITOR=0
DEPLOY_STORAGE=0
DEPLOY_BQ=0
DEPLOY_CODE=0
ACTIVATE_APIS=0
DEPLOY_RUNNERS=0
DEPLOY_OAUTH=0
CREATE_SERVICE_ACCOUNT=0

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
      DEPLOY_FETCHER=1
      DEPLOY_TRIGGER=1
      DEPLOY_MONITOR=1
      DEPLOY_LOADER=1
      DEPLOY_RUNNERS=1
      DEPLOY_BQ=1
      DEPLOY_STORAGE=1
      DEPLOY_OAUTH=1
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
    --deploy-runner)
      DEPLOY_RUNNERS=1
      ;;
    --deploy-storage)
      DEPLOY_STORAGE=1
      ;;
    --deploy-code)
      DEPLOY_CODE=1
      ;;
    --activate-apis)
      ACTIVATE_APIS=1
      ;;
    --create-service-account)
      CREATE_SERVICE_ACCOUNT=1
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


if [ ${ACTIVATE_APIS} -eq 1 ]; then
  # Check for active APIs
  APIS_USED=(
    "adsdatahub"
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
  gcloud iam service-accounts create report2bq --description "Report2BQ Service Account" --project ${PROJECT} 
  gcloud iam service-accounts keys create "report2bq@${PROJECT}.iam.gserviceaccount.com.json" --iam-account report2bq@${PROJECT}.iam.gserviceaccount.com --project ${PROJECT} 
  gsutil cp "report2bq@${PROJECT}.iam.gserviceaccount.com.json" gs://${PROJECT}-report2bq-tokens/
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

  # Create the zip
  ${DRY_RUN} zip report2bq.zip main.py requirements.txt README.md LICENSE classes/*.py cloud_functions/*.py screenshots/*.*

  # Copy it up
  ${DRY_RUN} gsutil cp report2bq.zip gs://${PROJECT}-report2bq > /dev/null 2>&1
  RETVAL=$?
  if (( ${RETVAL} != "0" )); then
    echo Error deploying zip file!
    exit
  fi
  SOURCE="gs://${PROJECT}-report2bq/report2bq.zip"
else
  SOURCE="https://source.developers.google.com/projects/${PROJECT}/repos/report2bq"
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
  ${DRY_RUN} gcloud functions deploy "job-monitor" \
    --entry-point=job_monitor \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-topic="job-monitor" \
    --quiet \
    --timeout=60s \
    --project=${PROJECT}

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
    --project=${PROJECT}
fi

# Fetcher
if [ ${DEPLOY_FETCHER} -eq 1 ]; then
  # Deploy cloud function
  ${DRY_RUN} gcloud functions deploy "report2bq-fetcher" \
    --entry-point=report_fetch \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-topic="report2bq-trigger" \
    --quiet \
    --timeout=540s \
    --project=${PROJECT}
fi

# Loader
if [ ${DEPLOY_LOADER} -eq 1 ]; then
  # Deploy cloud function
  ${DRY_RUN} gcloud functions deploy "report2bq-loader" \
    --entry-point=report_upload \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-resource="projects/_/buckets/${PROJECT}-report2bq-upload" \
    --trigger-event="google.storage.object.finalize" \
    --set-env-vars=BQ_DATASET=${DATASET} \
    --quiet \
    --timeout=540s \
    --project=${PROJECT}
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
  ${DRY_RUN} gcloud functions deploy "report-runner" \
    --entry-point=report_runner \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=2048MB \
    --trigger-topic="report-runner" \
    --quiet \
    --timeout=540s \
    --project=${PROJECT}

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
  ${DRY_RUN} gcloud functions deploy "run-monitor" \
    --entry-point=run_monitor \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-topic="run-monitor" \
    --quiet \
    --timeout=60s \
    --project=${PROJECT}

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

# Deploy runners
if [ ${DEPLOY_OAUTH} -eq 1 ]; then
  ${DRY_RUN} gcloud functions deploy "OAuth" \
    --entry-point=oauth \
    --allow-unauthenticated \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-http \
    --quiet \
    --timeout=60s \
    --project=${PROJECT}

  ${DRY_RUN} gcloud functions deploy "OAuthComplete" \
    --entry-point=oauth_complete \
    --allow-unauthenticated \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-http \
    --quiet \
    --timeout=60s \
    --project=${PROJECT}

  ${DRY_RUN} gcloud functions deploy "OAuthRequest" \
    --entry-point=oauth_request \
    --allow-unauthenticated \
    --source=${SOURCE} \
    --runtime python37 \
    --memory=1024MB \
    --trigger-http \
    --quiet \
    --timeout=60s \
    --project=${PROJECT}
fi