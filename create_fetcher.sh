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

# create_fetcher.sh
#
# Create the Cloud Scheduler job to add a new run/fetch report job to the project's Cloud
# Scheduler

# Functions
function usage() {
  cat << EOF
create_fetcher.sh
=================

Usage:
  create_fetcher.sh [options]

Options:
  Common
  ------
    --project     GCP Project Id
    --email       Email address attached to the OAuth token stored in GCS
    --runner      Create a report runner rather than a report fetcher

  DV360/CM
  --------
    --report-id   Id of the DV360/CM report

  CM Only
  -------
    --profile     The Campaign Manager profle id under which the report is defined

  SA360 Only
  ----------
    --sa360-url   The URL of the web download report in SA360. This will be in the format
                  https://searchads.google.com/ds/reports/download?ay=xxxxxxxxx&av=0&rid=000000&of=webquery

  ADH Only
  --------
    --adh-customer
                  The ADH customer id (no dashes)
    --adh-query   The ADH Query Id
    --api-key     ADH API Key, created in the GCP Console
    --days        Days lookback for the ADH report. This defaults to 60.
    --dest-project
                  Destination GCP project for ADH query results
    --dest-dataset
                  Destination BQ dataset for ADH query results

  Other
  -----
    --force       Force the report to upload EVERY TIME rather than just when updates are detected
    --rebuild-schema
                  Force the report to redefine the schema by re-reading the CSV header. Use with care.
                  This is incompatible with 'append' since a schema change will cause a drop of the
                  original table. Should really not be placed in an hourly fetcher.
    --dry-run     Don't do anything, just print the commands you would otherwise run. Useful 
                  for testing.
    --append      Append to the existing table instead of overwriting
    --timer       Set the specific minute at which the job will run. Random if not specified.
    --hour        Set the specific hour at which the job will run. 
                  For a DV360/CM fetcher, this is '*', or 'every hour' at 'timer' minute and 
                  cannot be changed
                  For ADH this defaults to 2
                  For SA360 this defaults to 3
                  For report runners this defaults to 1
    --description Plain text description for the scheduler list

    --usage       Show this text
EOF
}

# Switch definitions
PROJECT="galvanic-card-234919"      # Project id
FORCE=                              # Force an install overrwrite of objects
FETCHER=                            # 'Fetcher' name
REBUILD_SCHEMA=                     # issue a --rebuild_schema directive. Use with care
TRIGGER="report2bq-trigger"         # Name of the trigger PubSub queue
let "TIMER=$RANDOM % 59"            # Random minute timer
APPEND=                             # Set to make the installed fetcher append to existing data in BQ
HOUR=""                             # Hour at which to run
DAYS=60                             # Default day lookback for ADH
IS_RUNNER=0                         # Is this a 'run' or a 'fetch'
DEST_PROJECT=                       # Destination project for ADH querys
DEST_DATASET=                       # Destination dataset for ADH querys

# Command line parameter parser
while [[ $1 == -* ]] ; do
  case $1 in 
    # Common to SA360, DV360, ADH and CM
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --email*)
      IFS="=" read _cmd EMAIL <<< "$1" && [ -z ${EMAIL} ] && shift && EMAIL=$1
      ;;
    --runner)
      IS_RUNNER=1
      ;;

    # DV360 and CM
    --report-id*)
      IFS="=" read _cmd REPORT_ID <<< "$1" && [ -z ${REPORT_ID} ] && shift && REPORT_ID=$1
      ;;

    # CM only
    --profile*)
      IFS="=" read _cmd PROFILE <<< "$1" && [ -z ${PROFILE} ] && shift && PROFILE=$1
      ;;

    # ADH only
    --adh-customer*)
      IFS="=" read _cmd ADH_CUSTOMER <<< "$1" && [ -z ${ADH_CUSTOMER} ] && shift && ADH_CUSTOMER=$1
      ;;
    --adh-query*)
      IFS="=" read _cmd ADH_QUERY <<< "$1" && [ -z ${ADH_QUERY} ] && shift && ADH_QUERY=$1
      ;;
    --api-key*)
      IFS="=" read _cmd API_KEY <<< "$1" && [ -z ${API_KEY} ] && shift && API_KEY=$1
      ;;
    --days*)
      IFS="=" read _cmd DAYS <<< "$1" && [ -z ${DAYS} ] && shift && DAYS=$1
      ;;
    --dest-project*)
      IFS="=" read _cmd DEST_PROJECT <<< "$1" && [ -z ${DEST_PROJECT} ] && shift && DEST_PROJECT=$1
      ;;
    --dest-dataset*)
      IFS="=" read _cmd DEST_DATASET <<< "$1" && [ -z ${DEST_DATASET} ] && shift && DEST_DATASET=$1
      ;;

    # SA360 only
    --sa360-url*)
      IFS="=" read _cmd SA360_URL <<< "$1" && [ -z ${SA360_URL} ] && shift && SA360_URL=$1
      # SA360_URL=rawurlencode ${SA360_URL}
      ;;

    # Optional  
    --force)
      FORCE="force=True"
      ;;
    --rebuild-schema)
      REBUILD_SCHEMA="rebuild_schema=True"
      ;;
    --dry-run)
      DRY_RUN=echo
      ;;
    --append)
      APPEND="append=True"
      ;;
    --timer*)  
      # Random if not set
      IFS="=" read _cmd TIMER <<< "$1" && [ -z ${TIMER} ] && shift && TIMER=$1
      ;;
    --hour*)  
      # '*' if not set
      IFS="=" read _cmd HOUR <<< "$1" && [ -z ${HOUR} ] && shift && HOUR=$1
      ;;
    --description*)
      IFS="=" read _cmd DESCRIPTION <<< "$1" && [ -z "${DESCRIPTION}" ] && shift && DESCRIPTION="$1"
      ;;
    --help)
      usage
      exit
      ;;
    *)
      usage
      echo ""
      echo "Unknown parameter $1"
  esac
  shift
done

if [ "x${REPORT_ID}" == "x" -a "x${SA360_URL}" == "x" -a "x${ADH_CUSTOMER}" == "x" ]; then
  usage
  echo ""
  echo You must specify a report id or SA360 url.
  exit
fi

if [ -z ${EMAIL} ]; then
  usage
  echo ""
  echo You must specify am email address.
  exit
fi

parameters=(
  "${FORCE}"
  "${REBUILD_SCHEMA}"
  "${APPEND}"
  "email=${EMAIL}"
  "project=${PROJECT}"
)

if [ "x${ADH_CUSTOMER}" != "x" ]; then 
  FETCHER="run-adh-${ADH_CUSTOMER}-${ADH_QUERY}"
  TRIGGER="report-runner"
  [ -z "${DEST_PROJECT}" ] || _DEST_PROJECT="dest_project=${DEST_PROJECT}"
  [ -z "${DEST_DATASET}" ] || _DEST_DATASET="dest_dataset=${DEST_DATASET}"
  parameters=(
    ${parameters[@]}
    "adh_customer=${ADH_CUSTOMER}"
    "adh_query=${ADH_QUERY}"
    "api_key=${API_KEY}"
    "days=${DAYS}"
    "type=adh"
    "${_DEST_PROJECT}"
    "${_DEST_DATASET}"
  )
  case ${HOUR} in
    "")
      HOUR=2
      ;;
    *)
      HOUR="*"
      ;;
  esac
elif [ "x${SA360_URL}" != "x" ]; then 
  # SA360
  id_regex='^.*rid=([0-9]+).*$'
  [[ ${SA360_URL} =~ $id_regex ]]
  ID=${BASH_REMATCH[1]}
  FETCHER="fetch-sa360-${ID}"
  TRIGGER="report2bq-trigger"
  parameters=(
    ${parameters[@]}
    "sa360_url=${SA360_URL}"
  )
  case ${HOUR} in
    "")
      HOUR=3
      ;;
    *)
      HOUR="*"
      ;;
  esac
elif [ "x${PROFILE}" == "x" ]; then
  # DV360
  if [[ ${IS_RUNNER} -eq 1 ]]; then
    TRIGGER="report-runner"
    NAME="run"
    [ "${HOUR}" -eq "*" ] && HOUR=1
  else
    TRIGGER="report2bq-trigger"
    NAME="fetch"
    HOUR="*"
  fi

  FETCHER="${NAME}-dv360-${REPORT_ID}"
  parameters=(
    ${parameters[@]}
    "dv360_id=${REPORT_ID}"
    "type=dv360"
  )
else
  # CM
  if [[ ${IS_RUNNER} -eq 1 ]]; then
    TRIGGER="report-runner"
    NAME="run"
    [ "${HOUR}" == "*" ] && HOUR=1
  else
    TRIGGER="report2bq-trigger"
    NAME="fetch"
    HOUR="*"
  fi

  FETCHER="${NAME}-cm-${REPORT_ID}"
  parameters=(
    ${parameters[@]}
    "cm_id=${REPORT_ID}"
    "profile=${PROFILE}"
    "type=dcm"
  )
fi

if [ "x${FETCHER}" != "x" ]; then
  ATTRIBUTES=
  for i in ${parameters[@]}; do
    if [ "x${ATTRIBUTES}" == "x" ]; then
      ATTRIBUTES=${i}
    else
      ATTRIBUTES="${ATTRIBUTES},${i}"
    fi
  done

  for w in ${DESCRIPTION[@]}; do
    if [ -z "${_DESC}" ]; then
      _DESC=${w}
    else
      _DESC="${_DESC} ${w}"
    fi
  done

  # # Create scheduled jobs
  ${DRY_RUN} gcloud beta scheduler jobs delete \
    --project=${PROJECT} \
    --quiet \
    "${FETCHER}"

  ${DRY_RUN} gcloud beta scheduler jobs create pubsub \
    "${FETCHER}" \
    --schedule="${TIMER} ${HOUR} * * *" \
    --topic="projects/${PROJECT}/topics/${TRIGGER}" \
    --attributes="${ATTRIBUTES}" \
    --time-zone="America/Toronto" \
    --message-body="RUN" \
    --description="${_DESC}" \
    --project=${PROJECT}
fi
