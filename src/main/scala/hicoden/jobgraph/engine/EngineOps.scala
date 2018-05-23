package hicoden.jobgraph.engine

import hicoden.jobgraph.configuration.step.JobDescriptorTable
import hicoden.jobgraph.configuration.workflow.WorkflowDescriptorTable
import hicoden.jobgraph.configuration.workflow.internal.Concretizer
import hicoden.jobgraph.configuration.workflow.{Parser ⇒ WfParser, Loader ⇒ WfLoader }
import hicoden.jobgraph.configuration.step.{Parser ⇒ StepParser, Loader ⇒ StepLoader }
import hicoden.jobgraph.configuration.step.model._
import hicoden.jobgraph.configuration.workflow.model._
import hicoden.jobgraph.{Job, JobId}

import quiver._

/**
 * Responsible for taking the hydrated workflow configs and instantiating the
 * proper jobgraphs.
 *
 * @author Raymond Tay
 * @verison 1.0
 */
trait EngineOps extends Concretizer {

  import cats._, data._, implicits._

  object StepOps extends StepParser with StepLoader
  object WfOps extends WfParser with WfLoader

  /**
   * Called when the [[Engine]] actor boots up, reading and validating the
   * configuration
   * @param stepNamespaces
   * @param workflowNamespaces
   * @return 2-tuple where the [[JobDescriptorTable]] and
   * [[WorkflowDescriptorTable]] is returned
   */
  def prepareDescriptorTables(stepNamespaces: List[String], workflowNamespaces: List[String]) : (JobDescriptorTable, WorkflowDescriptorTable) = {
    val loadedJobConfigs = StepOps.loadDefault(stepNamespaces) /* loads the configuration from the [[application.conf]] */
    var jdt : JobDescriptorTable = scala.collection.immutable.HashMap.empty[Int, JobConfig]
    jdt = StepOps.hydrateJobConfigs(loadedJobConfigs.toList.flatten).runS(jdt).value

    val loadedWfConfigs = WfOps.loadDefault(workflowNamespaces) /* this loads the configuration from [[application.conf]] */
    var wfdt : WorkflowDescriptorTable = scala.collection.immutable.HashMap.empty[Int, WorkflowConfig]
    wfdt = WfOps.hydrateWorkflowConfigs(loadedWfConfigs.toList.flatten).runS(wfdt).value

    (jdt, wfdt)
  }

  /**
   * Attempt to load the workflow by indexing its index in the configuration
   * file that was loaded (remember, by default its [[application.conf]]) 
   * @param workflowIndex
   * @param jdt
   * @param wfdt
   * @return Some((List of [[LNode]], List of [[LEdge]])) or none
   */
  def extractWorkflowConfigBy(workflowIndex: Int)(implicit jdt : JobDescriptorTable, wfdt: WorkflowDescriptorTable) : Option[(List[LNode[Job,JobId]], List[LEdge[Job,String]])]= {
    if (wfdt.contains(workflowIndex)) reify(jdt)(wfdt(workflowIndex)).toOption
    else none
  }

}
