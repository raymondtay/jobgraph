package hicoden.jobgraph.configuration.workflow.model

/**
  * Using [PureConfig](https://github.com/pureconfig/pureconfig) to lift the
  * boilerplate code into data models which can be leveraged efficiently here.
  */
case class WorkflowConfig(id : Int, name : String, description : String, jobgraph : List[String])

/**
  * This model captures and represents what is considered overrideable as
  * described in https://nugitco.atlassian.net/wiki/spaces/ND/pages/437846029/Job+Specification
  */
case class JobOverrides(
  id            : Int, // indicate to the system which job you want to be overrided when it executes
  description   : Option[String],
  workdir       : Option[String],
  sessionid     : Option[String],
  runnerRunner  : Option[String],
  runnerCliArgs : Option[List[String]]
)

// This model supports the user to be able to override the defaults (i.e.
// static configuration files or dynamically generated via the REST interface)
//
case class JobConfigOverrides(
  overrides : List[JobOverrides]
)

