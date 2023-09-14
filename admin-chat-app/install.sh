#!/bin/bash

# Functions
function usage() {
  cat << EOF
install.sh
==========

Usage:
  install.sh [options]

Options:
  --project         GCP Project Id.
  --job-manager-uri The Report2BQ Job Manager URI.
  --service-account The service account to use to run the admin app. This
                    should be the same as your main Report2BQ's service
                    account.
General switches:
  --dry-run         Don't do anything, just print the commands you would
                    otherwise run. Useful for testing.
  --usage           Show this text
EOF
}

function join() { local IFS="$1"; shift; echo "$*"; }

function jsonq() { python -c "import sys,json; obj=json.load(sys.stdin); print($1)"; }

# JOB_MANAGER_URI=https://us-central1-chats-zz9-plural-z-alpha.cloudfunctions.net/report2bq-job-manager-http
# PROJECT=chats-zz9-plural-z-alpha
# USER=chats-zz9-plural-z-alpha@appspot.gserviceaccount.com
JOB_MANAGER_URI=
PROJECT=
USER=

# Command line parser
while [[ $1 == -* ]] ; do
  case $1 in
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --job-manager-uri*)
      IFS="=" read _cmd JOB_MANAGER_URI <<< "$1" && [ -z ${JOB_MANAGER_URI} ] && shift && JOB_MANAGER_URI=$1
      ;;
    --service-account*)
      IFS="=" read _cmd USER <<< "$1" && [ -z ${USER} ] && shift && USER=$1
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

SOURCE=`pwd`
PYTHON_RUNTIME=python310

_ENV_VARS=(
  "GCP_PROJECT=${PROJECT}"
  "JOB_MANAGER_URI=${JOB_MANAGER_URI}"
)

_DEFAULT_REGION=$(gcloud functions regions list --limit 1 --format json | jsonq 'obj[0]["locationId"]')
REDIRECT_URI="https://${_DEFAULT_REGION}-${PROJECT}.cloudfunctions.net/report2bq-oauth-complete"
_ENV_VARS+=("REDIRECT_URI=${REDIRECT_URI}")
ENVIRONMENT=$(join "," ${_ENV_VARS[@]})

${DRY_RUN} gcloud functions deploy "report2bq-oauth-start"  \
  --entry-point=start_oauth                           \
  --service-account=${USER}                           \
  --runtime ${PYTHON_RUNTIME}                         \
  --source=${SOURCE}                                  \
  --set-env-vars=${ENVIRONMENT}                       \
  --memory=4096MB                                     \
  --timeout=240s                                      \
  --trigger-http                                      \
  --allow-unauthenticated                             \
  --quiet                                             \
  --project=${PROJECT}

${DRY_RUN} gcloud functions deploy "report2bq-oauth-complete"  \
  --entry-point=complete_oauth                        \
  --service-account=${USER}                           \
  --runtime ${PYTHON_RUNTIME}                         \
  --source=${SOURCE}                                  \
  --set-env-vars=${ENVIRONMENT}                       \
  --memory=4096MB                                     \
  --timeout=240s                                      \
  --trigger-http                                      \
  --allow-unauthenticated                             \
  --quiet                                             \
  --project=${PROJECT}


${DRY_RUN} gcloud functions deploy "report2bq-admin"  \
  --entry-point=report2bq_admin                       \
  --service-account=${USER}                           \
  --runtime ${PYTHON_RUNTIME}                         \
  --source=${SOURCE}                                  \
  --set-env-vars=${ENVIRONMENT}                       \
  --memory=4096MB                                     \
  --timeout=240s                                      \
  --trigger-http                                      \
  --allow-unauthenticated                             \
  --quiet                                             \
  --project=${PROJECT}
