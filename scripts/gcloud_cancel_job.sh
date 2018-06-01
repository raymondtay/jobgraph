#!/bin/sh

# gcloud sdk should be installed prior to use
# https://cloud.google.com/sdk/docs/quickstart-linux

# !!! NOTE !!!
# Read this https://cloud.google.com/dataflow/pipelines/stopping-a-pipeline
# to understand the differences between "cancelling" vs. "draining" a job
# It has impacts on costs


# the ENV should contain a JOB_ID which points to the job id in question
gcloud --format=json --quiet beta dataflow jobs cancel $JOB_ID

