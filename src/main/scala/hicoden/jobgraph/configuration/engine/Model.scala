package hicoden.jobgraph.configuration.engine.model

/**
  * see [[application.conf]] for details on the namespace 'mesos' and 'jobgraph'
  *  mesos {
  *   enabled         : true
  *   runas           : hicoden
  *   master.hostname : 10.148.0.4
  *   master.hostname : ${?JOBGRAPH_MASTER_HOST}
  *   master.hostport : 5050
  *   master.hostport : ${?JOBGRAPH_MASTER_PORT}
  *  }
  * jobgraph {
  *   hostname : 10.148.0.2
  *   hostname : ${?JOBGRAPH_HOST}
  *   hostport : 9000
  *   hostport : ${?JOBGRAPH_PORT}
  * }
  *
  */
case class MesosConfig(enabled: Boolean, runas: String, hostname: String, hostport: Int)
case class JobgraphConfig(hostname : String, hostport : Int)

