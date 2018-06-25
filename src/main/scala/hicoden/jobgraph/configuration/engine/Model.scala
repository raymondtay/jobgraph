package hicoden.jobgraph.configuration.engine.model

/**
  * see [[application.conf]] for details on the namespace 'mesos'
  *  mesos {
  *   enabled         : true
  *   runas           : hicoden
  *   master.hostname : 10.148.0.4
  *   master.hostname : ${?JOBGRAPH_MASTER_HOST}
  *   master.hostport : 5050
  *   master.hostport : ${?JOBGRAPH_MASTER_PORT}
  *  }
  */
case class MesosConfig(enabled: Boolean, runas: String, hostname: String, hostport: Int)

