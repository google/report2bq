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
    --dest-project
                  Destination GCP project (if different than "--project")
    --dest-dataset
                  Destination BQ dataset (if not 'report2bq')

  DV360/CM
  --------
    --report-id   Id of the DV360/CM report

  CM Only
  -------
    --profile     The Campaign Manager profle id under which the report is defined

  SA360 Web Download Report Only
  ------------------------------
    --sa360-url   The URL of the web download report in SA360. This will be in the format
                  https://searchads.google.com/ds/reports/download?ay=xxxxxxxxx&av=0&rid=000000&of=webquery

  SA360 Dynamic Report Only
  -------------------------
    --sa360-id    The UUID of the created SA360 report

  ADH Only
  --------
    --adh-customer
                  The ADH customer id (no dashes)
    --adh-query   The ADH Query Id
    --api-key     ADH API Key, created in the GCP Console
    --days        Days lookback for the ADH report. This defaults to 60.

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
    --time-zone   Timezone for the job. Default is the value in /etc/timezone, or UTC if that file is
                  not present. If you type it manually, the value of this field must be a time zone
                  name from the TZ database (http://en.wikipedia.org/wiki/Tz_database)
    --description Plain text description for the scheduler list
    --infer-schema
                  [BETA] Guess the column types based on a sample of the report's first slice.
    --topic       [BETA] Topic to send a PubSub message to on completion of import job
    --message     [BETA] Message to send; this should be the name of the custom function to be
                  executed. Attributes of dataset, table name, report id and report type will always
                  be sent along with this as part of the message.
    --usage       Show this text
EOF
}

function set_hour {
  # Check for a valid hour in the ${HOUR} variable from the command line.
  # If there isn't one, use the default value supplied in $1. If $1 is not
  # supplied, default to '*'.
  if [[ $HOUR =~ ^[0-9]+$ ]]; then
    if !(($HOUR >= 0 && $HOUR <= 23)); then
      HOUR=$1
    fi
  else
    if [[ $1 =~ ^[0-9]+$ ]]; then
      HOUR=$1
    else
      HOUR="*"
    fi
    return
  fi
}

# Switch definitions
PROJECT=                            # Project id
FORCE=                              # Force an install overwrite of objects
FETCHER=                            # 'Fetcher' name
REBUILD_SCHEMA=                     # issue a --rebuild_schema directive. Use with care
TRIGGER="report2bq-trigger"         # Name of the trigger PubSub queue
let "TIMER=$RANDOM % 59"            # Random minute timer
APPEND=                             # Set to make the installed fetcher append to existing data in BQ
HOUR=""                             # Hour at which to run
DAYS=60                             # Default day lookback for ADH
IS_RUNNER=0                         # Is this a 'run' or a 'fetch'
DEST_PROJECT=                       # Destination project
DEST_DATASET=                       # Destination dataset
TIMEZONE=                           # Timezone
INFER_SCHEMA=                       # Guess the report's schema
SA360_ID=                           # ID of the SA360 report to schedule
TOPIC=                              # Notifier topic for post import processing
MESSAGE=                            # Notifier message

# Command line parameter parser
QUIT=0
UNKNOWN=
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
    --dest-project*)
      IFS="=" read _cmd DEST_PROJECT <<< "$1" && [ -z ${DEST_PROJECT} ] && shift && DEST_PROJECT=$1
      ;;
    --dest-dataset*)
      IFS="=" read _cmd DEST_DATASET <<< "$1" && [ -z ${DEST_DATASET} ] && shift && DEST_DATASET=$1
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

    # SA360 only
    --sa360-url*)
      IFS="=" read _cmd SA360_URL <<< "$1" && [ -z ${SA360_URL} ] && shift && SA360_URL=$1
      # SA360_URL=rawurlencode ${SA360_URL}
      ;;
    --sa360-id*)
      IFS="=" read _cmd SA360_ID <<< "$1" && [ -z ${SA360_ID} ] && shift && SA360_ID=$1
      ;;

    # Optional
    --force)
      FORCE="force=True"
      ;;
    --infer-schema)
      INFER_SCHEMA="infer_schema=True"
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
    --time-zone*)
      IFS="=" read _cmd TIMEZONE <<< "$1" && [ -z "${TIMEZONE}" ] && shift && TIMEZONE="$1"
      ;;
    --topic*)
      IFS="=" read _cmd TOPIC <<< "$1" && [ -z "${TOPIC}" ] && shift && TOPIC="$1"
      ;;
    --message*)
      IFS="=" read _cmd MESSAGE <<< "$1" && [ -z "${MESSAGE}" ] && shift && MESSAGE="$1"
      ;;
    --help)
      usage
      exit
      ;;
    *)
      QUIT=1
      UNKNOWN=(
        ${UNKNOWN[@]}
        $1
      )
      QUIT=1
  esac
  shift
done

if [[ $QUIT -eq 1 ]]; then
  echo "Unknkown parameter(s): "
  echo ${UNKNOWN}
  echo ""
  usage
  exit
fi

if [ "x${REPORT_ID}" == "x" -a "x${SA360_URL}" == "x" -a "x${ADH_CUSTOMER}" == "x" -a "${SA360_ID}x" == "x" ]; then
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

[ -z "${DEST_PROJECT}" ] || _DEST_PROJECT="dest_project=${DEST_PROJECT}"
[ -z "${DEST_DATASET}" ] || _DEST_DATASET="dest_dataset=${DEST_DATASET}"
[ -z "${TOPIC}" ] || _NOTIFIER_TOPIC="notify_topic=${TOPIC}"
[ -z "${MESSAGE}" ] || _NOTIFIER_MESSAGE="notify_message=${MESSAGE}"

parameters=(
  "${FORCE}"
  "${REBUILD_SCHEMA}"
  "${APPEND}"
  "email=${EMAIL}"
  "project=${PROJECT}"
  "${_DEST_PROJECT}"
  "${_DEST_DATASET}"
  "${_NOTIFIER_TOPIC}"
  "${_NOTIFIER_MESSAGE}"
)

if [ ! -z "${TIMEZONE}" ]; then
  TZ=${TIMEZONE}
  _TZ="--time-zone=${TIMEZONE}"
else
  if [ -f /etc/timezone ]; then
    TZ=`cat /etc/timezone`
    _TZ="--time-zone=${TZ}"
  fi
fi

if [ "x${ADH_CUSTOMER}" != "x" ]; then
  FETCHER="run-adh-${ADH_CUSTOMER}-${ADH_QUERY}"
  TRIGGER="report-runner"
  parameters=(
    ${parameters[@]}
    "adh_customer=${ADH_CUSTOMER}"
    "adh_query=${ADH_QUERY}"
    "api_key=${API_KEY}"
    "days=${DAYS}"
    "type=adh"
  )
  set_hour 2
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
    "type=sa360"
  )
  set_hour 3
elif [ ! -z ${SA360_ID} ]; then
# SA360 dynamic report
  TRIGGER="report-runner"
  NAME="run"
  set_hour "*"
  FETCHER="${NAME}-sa360_report-${SA360_ID}"
  parameters=(
    ${parameters[@]}
    "report_id=${SA360_ID}"
    "type=sa360_report"
    "timezone=${TZ}"
  )
elif [ "x${PROFILE}" == "x" ]; then
  # DV360
  if [[ ${IS_RUNNER} -eq 1 ]]; then
    TRIGGER="report-runner"
    NAME="run"
    set_hour 1
  else
    TRIGGER="report2bq-trigger"
    NAME="fetch"
    HOUR="*"
    parameters=(
      ${parameters[@]}
      ${INFER_SCHEMA}
    )
  fi

  FETCHER="${NAME}-dv360-${REPORT_ID}"
  parameters=(
    ${parameters[@]}
    "report_id=${REPORT_ID}"
    "type=dv360"
  )
else
  # CM
  if [[ ${IS_RUNNER} -eq 1 ]]; then
    TRIGGER="report-runner"
    NAME="run"
    set_hour 1
  else
    TRIGGER="report2bq-trigger"
    NAME="fetch"
    HOUR="*"
    parameters=(
      ${parameters[@]}
      ${INFER_SCHEMA}
    )
  fi

  FETCHER="${NAME}-cm-${REPORT_ID}"
  parameters=(
    ${parameters[@]}
    "report_id=${REPORT_ID}"
    "profile=${PROFILE}"
    "type=cm"
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
    ${_TZ} \
    --message-body="RUN" \
    --description="${_DESC}" \
    --project=${PROJECT}
fi
