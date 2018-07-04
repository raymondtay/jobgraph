package hicoden.jobgraph.cc

import FrameworkStates.FrameworkState

// Extracted directly from "mesos-host:mesos-port/tasks.json"
// and lifting only the values we care for.
case class ClusterMetrics(
  framework_id : FrameworkId,
  state : FrameworkState
)

