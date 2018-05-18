#!/bin/sh

# For the purpose of using Tut plugin, do not use this script for anything
# else.

# gcloud sdk should be installed prior to use 
# https://cloud.google.com/sdk/docs/quickstart-linux

# the ENV should contain a JOB_ID which points to the job id in question
echo '{"job_id":"'$JOB_ID'"}'

