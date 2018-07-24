package hicoden.jobgraph.configuration.engine.model

/**
  * see [[application.conf]] for details on the namespace 'mesos' and 'jobgraph'
  *  mesos {
  *   enabled         : true
  *   timeout         : 10
  *   runas           : hicoden
  *   uris : ["10.148.0.7:5050", "10.148.0.7:5050"]
  *   uris : ${?MESOS_CONTACT_POINTS}
  *  }
  * jobgraph {
  *   hostname : 10.148.0.2
  *   hostname : ${?JOBGRAPH_HOST}
  *   hostport : 9000
  *   hostport : ${?JOBGRAPH_PORT}
  * }
  *
  * jobgraph.db {
  *   driver : "org.postgresql.Driver"
  *   name : "jobgraph",
  *   username : "postgres",
  *   password : "password"
  * }
  *
  */
case class MesosConfig(enabled: Boolean, timeout : Int, runas: String, uris: List[String])
case class JobgraphConfig(hostname : String, hostport : Int)
case class JobgraphDb(driver: String, name : String, username: String, password: String)

