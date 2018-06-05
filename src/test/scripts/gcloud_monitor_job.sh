#!/bin/sh

# !!!!! DUMMY SCRIPT !!!!!!!
# Used by the test to verify that [[DataflowMonitorRunnerSpecs.scala]] and [[FSMSpec.scala]] can actually run it

# gcloud sdk should be installed prior to use 
# https://cloud.google.com/sdk/docs/quickstart-linux

# The ENV should contain a JOB_ID which points to the job id in question
# The test should read back the job_id that was given which is good enough to
# make assertions.

# The data string is a sample of what the current Gcloud SDK supports
# This format is correct as of 5 June 2018

echo '
 {
  "createTime": "2018-05-31T06:24:09.605580Z",
  "currentState": "JOB_STATE_DONE",
  "currentStateTime": "2018-05-31T06:30:56.227404Z",
  "environment": {
    "userAgent": {
      "name": "Apache Beam SDK for Python",
      "support": {
        "status": "SUPPORTED",
        "url": "https://github.com/apache/beam/releases"
      },
      "version": "2.4.0"
    },
    "version": {
      "job_type": "PYTHON_BATCH",
      "major": "7"
    }
  },
  "id": "'$JOB_ID'"
  }
'

