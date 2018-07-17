package hicoden.jobgraph.configuration.step.model

/**
  * Using [PureConfig](https://github.com/pureconfig/pureconfig) to lift the
  * boilerplate code into data models which can be leveraged efficiently here.
  * 
  * Take a moment to understand how the mapping is being done using this
  * example from the (sample) configuration file:
  *
  * runner {
  *   module : "apache_beam.examples.wordcount"
  *   runner : "Dataflow:python"
  *   cliargs : ["--jobName bloody-google-dataflow",
  *              "--input gs://hicoden/README.md",
  *              "--output gs://hicoden-test-1/XX",
  *              "--runner DataflowRunner",
  *              "--project hicoden",
  *              "--temp_location gs://hicoden-test-1/",
  *              "--callback http://0.0.0.0:9000/flow/<wfId>/job/<jobId>"
  *             ]
  *   cliargs : ${?CLI_ARGS}
  * }
  *
  */
case class Runner(module: String, runner: String, cliargs: List[String])
case class Restart(max : Int)
case class JobConfig(id : Int,
                     name : String,
                     description: String,
                     workdir : String,
                     sessionid : String,
                     restart : Restart,
                     runner: Runner,
                     inputs : List[String], outputs: List[String] )

object RunnerType extends Enumeration {
  type RunnerType = Value
  val Dataflow, MesosDataflow = Value
}

object ExecType extends Enumeration {
  type ExecType = Value
  val python, java = Value
}

