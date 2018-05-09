package hicoden.jobgraph.configuration

package object workflow {

  import hicoden.jobgraph.configuration.workflow.model.WorkflowConfig
  import pureconfig._

  // Simple map of mapping workflow numbers to their configuration
  type WorkflowDescriptorTable = collection.immutable.HashMap[Int, WorkflowConfig]

}
