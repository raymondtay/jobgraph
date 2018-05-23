package hicoden.jobgraph.fsm.runners

package object runtime {

  import hicoden.jobgraph.configuration.step.model.Runner

  // This is pretty much the runtime representation of the job's configuration
  // in [[JobConfig]].
  case class JobContext(name: String, description: String, workdir: String, sessionid: String, runner: Runner)

}
