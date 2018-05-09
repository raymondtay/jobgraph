package hicoden.jobgraph.configuration

package object step {

  import hicoden.jobgraph.configuration.step.model.JobConfig
  import pureconfig._

  // Simple map of mapping job numbers to their configuration
  type JobDescriptorTable = collection.immutable.HashMap[Int, JobConfig]

}
