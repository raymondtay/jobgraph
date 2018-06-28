package hicoden.jobgraph.engine

import hicoden.jobgraph.WorkflowOps
import hicoden.jobgraph.configuration.engine.{Parser ⇒ EngineConfigParser}
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
 * proper jobgraphs. Loads the engine's configuration too.
 *
 * @author Raymond Tay
 * @verison 1.0
 */
trait EngineOps extends Concretizer {

  import cats._, data._, implicits._

  import WorkflowOps._
  object StepOps extends StepParser with StepLoader
  object WfOps extends WfParser with WfLoader
  object EngineCfgParser extends EngineConfigParser

  /**
    * Loads the Apache Mesos Config
    * @returns Left(validation errors) or Right(MesosConfig)
    */
  def loadMesosConfig = EngineCfgParser.loadMesosDefaults("mesos")
 
  /**
    * Loads the Jobgraph engine's Config
    * @returns Left(validation errors) or Right(JobgraphConfig)
    */
  def loadEngineConfig = EngineCfgParser.loadEngineDefaults("jobgraph")

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
    * Returns all workflows currently present in the system; pagination
    * mechanism would be implemented at a later stage.
    * @param wfdt
    * @return a container where each value is a (k,v) pair
    */
  def getAllWorkflows : State[WorkflowDescriptorTable, List[WorkflowConfig]] =
    for {
      s ← State.get[WorkflowDescriptorTable]
    } yield s.values.toList

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

  /**
    * Validate the workflow submission by checking whether it is valid (i.e.
    * all job nodes referenced must exist and there's no loop since we want
    * DAGs)
    * @param wfConfig
    * @return Some(<workflow index>) else none
    */
  def validateWorkflowSubmission(implicit jdt: JobDescriptorTable, wfdt: WorkflowDescriptorTable) : Reader[WorkflowConfig, Option[WorkflowConfig]] =
    Reader{ (wfConfig: WorkflowConfig) ⇒
      if (isWorkflowIndexExisting(wfdt)(wfConfig)) none
      else
      reify(jdt)(wfConfig).fold(
        errors ⇒ none,
        (nodeEdges: (List[quiver.LNode[Job,JobId]], List[LEdge[Job,String]])) ⇒ {
          val jobgraph = mkGraph(nodeEdges._1, nodeEdges._2)
          if (jobgraph.isEmpty || jobgraph.hasLoop) {
            logger.error(s"[Engine][validateWorkflowSubmission] The graph is either empty or contains a loop:")
            none
          } else {
            logger.info(s"[Engine][validateWorkflowSubmission] The submitted workflow submission appears to be valid.")
            wfConfig.some
          }
        }
      )
    }

  // Detects whether the workflow is already present in the system i.e.
  // in-memory.
  private
  def isWorkflowIndexExisting(implicit wfdt: WorkflowDescriptorTable) : Reader[WorkflowConfig, Boolean] =
    Reader{ (wfConfig: WorkflowConfig) ⇒
      if (wfdt.contains(wfConfig.id)) true else false
    }

  /**
    * State function that adds the workflow configuration to the workflow
    * descriptor table and returns this updated version.
    * @param wfConfig
    * @param wfdt
    * @return updated Workflow Descriptor Table
    */
  def addNewWorkflow = Reader{ (wfConfig: WorkflowConfig) ⇒
    for {
      s  ← State.get[WorkflowDescriptorTable]
      _  ← State.modify{(dt: WorkflowDescriptorTable) ⇒
             WfOps.hydrateWorkflowConfigs(wfConfig :: Nil).runS(dt).value
           }
      s2 ← State.get[WorkflowDescriptorTable]
    } yield s2
  }
}

