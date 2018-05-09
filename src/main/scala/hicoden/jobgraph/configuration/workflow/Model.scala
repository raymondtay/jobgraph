package hicoden.jobgraph.configuration.workflow.model

/**
  * Using [PureConfig](https://github.com/pureconfig/pureconfig) to lift the
  * boilerplate code into data models which can be leveraged efficiently here.
  */
case class WorkflowConfig(id : Int, name : String, description : String, steps : List[Int], jobgraph : List[String])

