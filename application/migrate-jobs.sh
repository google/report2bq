#!/bin/bash

while [[ $1 == -* ]] ; do
  case $1 in
    --project*)
      IFS="=" read _cmd PROJECT <<< "$1" && [ -z ${PROJECT} ] && shift && PROJECT=$1
      ;;
    --dry-run)
      DRY_RUN=echo
      ;;
    *)
      echo "Unknown parameter $1."
      exit
  esac
  shift
done

if [ -z "${PROJECT}" ]; then
  usage
  echo ""
  echo You must specify a project to proceed.
  exit
fi

JOBS=$(gcloud --project=${PROJECT} scheduler jobs list)
export IFS=$'\n'

for JOB in ${JOBS}; do
  _SPLIT=$(echo ${JOB} | sed -re 's/^([a-z0-9_-]+).* ([A-Z]+)$/\1 \2/')
  IFS=$' ' read NAME STATUS <<< ${_SPLIT}
  echo Job: ${NAME}
  if [[ ${NAME} =~ ^run-.* ]]; then
    [[ ${STATUS} == 'PAUSED' ]] && ${DRY_RUN} gcloud --project=${PROJECT} scheduler jobs resume ${NAME}
    ${DRY_RUN} gcloud --project=${PROJECT} scheduler jobs update pubsub \
      ${NAME} --topic=report2bq-runner
    [[ ${STATUS} == 'PAUSED' ]] && ${DRY_RUN} gcloud --project=${PROJECT} scheduler jobs pause ${NAME}
  elif [[ ${NAME} =~ ^fetch-.* ]]; then
    [[ ${STATUS} == 'PAUSED' ]] && ${DRY_RUN} gcloud --project=${PROJECT} scheduler jobs resume ${NAME}
    ${DRY_RUN} gcloud --project=${PROJECT} scheduler jobs update pubsub \
      ${NAME} --topic=report2bq-fetcher
    [[ ${STATUS} == 'PAUSED' ]] && ${DRY_RUN} gcloud --project=${PROJECT} scheduler jobs pause ${NAME}
  fi
done
