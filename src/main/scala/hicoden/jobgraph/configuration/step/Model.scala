package hicoden.jobgraph.configuration.step.model

/**
  * Using [PureConfig](https://github.com/pureconfig/pureconfig) to lift the
  * boilerplate code into data models which can be leveraged efficiently here.
  */
case class Runner(jobjar: String, runner: String, cliargs: String)
case class JobConfig(id : Int, name : String, description: String, workdir : String, sessionid : String, runner: Runner, inputs : List[String], outputs: List[String] )

