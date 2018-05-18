#!/bin/sh

# gcloud sdk should be installed prior to use 
# https://cloud.google.com/sdk/docs/quickstart-linux

# the ENV should contain a JOB_ID which points to the job id in question
gcloud --format=json --quiet beta dataflow jobs describe $JOB_ID

