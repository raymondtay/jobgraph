#!/bin/sh

# !!!!! DUMMY SCRIPT !!!!!!!
# Used by the test to verify that [[DataflowJobTerminationRunner]] can actually run it

# gcloud sdk should be installed prior to use 
# https://cloud.google.com/sdk/docs/quickstart-linux

# The return string here is not accidental because this is the exact response
# from Google when it is successful.
echo "Failed to cancel job [$JOB_ID]"

