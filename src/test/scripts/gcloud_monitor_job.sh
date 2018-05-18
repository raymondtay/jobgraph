#!/bin/sh

# !!!!! DUMMY SCRIPT !!!!!!!
# Used by the test to verify that [[DataflowRunner]] can actually run it

# gcloud sdk should be installed prior to use 
# https://cloud.google.com/sdk/docs/quickstart-linux

# The ENV should contain a JOB_ID which points to the job id in question
# The test should read back the job_id that was given which is good enough to
# make assertions.

echo $JOB_ID

