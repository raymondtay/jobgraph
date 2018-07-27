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
import hicoden.jobgraph.engine.persistence._

import doobie._
import quiver._

/**
 * Responsibilities:
 * - Loads the hydrates the workflow and job configurations (in-memory and
 *   persistent storage)
 * - Loads the engine's configuration (in-memory)
 *
 * @author Raymond Tay
 * @verison 1.0
 */
trait EngineOps extends Concretizer with DatabaseOps {

  import cats._, data._, implicits._
  import hicoden.jobgraph.engine.runtime._

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
    * Craft the database sql object for a run
    * @param jdt
    * @return ConnectionIO[Int]
    */
  def fillDatabaseJobConfigs : Reader[JobDescriptorTable, ConnectionIO[Int]] =
    Reader{ (jdt: JobDescriptorTable) ⇒
      import doobie.implicits._
      jdt.values.map(jobConfigOp(_)).reduce(_ ++ _).update.run
    }
  
  def fillDatabaseWorkflowConfigs : Reader[WorkflowDescriptorTable, ConnectionIO[Int]] =
    Reader{ (wfdt: WorkflowDescriptorTable) ⇒
      import doobie.implicits._
      wfdt.values.map(workflowConfigOp(_)).reduce(_ ++ _).update.run
    }


  /**
    * The primary validation scheme here would be to make sure the runners
    * indicated is legal and the job ids indicated in the payload do exist in
    * our current system configuration; it doesn't make much sense to validate
    * the rest of the overrides because they are context-dependent i.e. only
    * the job that needs these overrides would know exactly what to do with it.
    *
    * CAUTION: as the job descriptor table is not concurrent safe, which means
    * there is a chance that we might report missing job entries in the system
    * @param overrides
    * @return Left(validation-failure) or Right(true)
    */
  def validateJobOverrides(jdt: JobDescriptorTable) : Reader[JobConfigOverrides, Option[List[Int]]] =
    Reader{ (overrides:JobConfigOverrides) ⇒
      if (jdt.isEmpty) none
      else {
        val incoming = Set(overrides.overrides.collect{ case c => c.id }:_*)
        val intersect = jdt.keySet & incoming

        if (intersect.isEmpty) none // absolutely nothing in common
        else if ((intersect & incoming) == intersect) incoming.toList.some // everything indicated is there
        else none // accounts for everything else
      }
    }

  /**
    * Returns all workflows currently present in the system; pagination
    * mechanism would be implemented at a later stage.
    * @param wfdt
    * @return a container where each value is a [[WorkflowConfig]] object
    */
  def getAllWorkflows : State[WorkflowDescriptorTable, List[WorkflowConfig]] =
    for {
      s ← State.get[WorkflowDescriptorTable]
    } yield s.values.toList

  /**
    * Returns all job configurations currently present in the system; pagination
    * mechanism would be implemented at a later stage.
    * @param wfdt
    * @return a container where each value is a [[JobConfig]] object
    */
  def getAllJobs : State[JobDescriptorTable, List[JobConfig]] =
    for {
      s ← State.get[JobDescriptorTable]
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
    * Validate the job submission means that we check for a few things
    * - The job id must be distinct from the rest since it is the same id that
    *   is used to reference the job in the workflow DAG
    * - The dataflow must contain the right [[RunnerType]] [[ExecType]] pair
    *   else it is consider illegal
    * @param jobConfig
    * @return Some(jobConfig) else none
    */
  def validateJobSubmission(implicit jdt: JobDescriptorTable) : Reader[JobConfig, Option[JobConfig]] =
    Reader{ (jCfg: JobConfig) ⇒
      if (isJobConfigPresent(jdt)(jCfg)) none else {
        val splitted = jCfg.runner.runner.split(":")
        if (splitted.size != 2) { logger.error(s"Runner configuration is invalid"); none }
        else {
          val (left, right) = (splitted(0), splitted(1))
          (StepOps.validateRunnerType(left), StepOps.validateExecType(right)).mapN((r, e) ⇒
            if (r == None || e == None) { logger.error(s"Runner / Exec type configuration is invalid"); none } else jCfg.some
          )
        }
      }
    }

  // The JDT is a indexed structure; so a check is made to see if its already
  // there - the user cannot override the configuration.
  private def isJobConfigPresent(implicit jdt: JobDescriptorTable) = Reader{ (jCfg: JobConfig) ⇒
    if (jdt.contains(jCfg.id)) true else false
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

  /**
    * State function that adds the job configuration to the job
    * descriptor table and returns this updated version.
    * @param jobConfig
    * @param jdt
    * @return updated Job Descriptor Table
    */
  def addNewJob = Reader{ (jobConfig: JobConfig) ⇒
    for {
      s  ← State.get[JobDescriptorTable]
      _  ← State.modify{(dt: JobDescriptorTable) ⇒
             StepOps.hydrateJobConfigs(jobConfig :: Nil).runS(dt).value
           }
      s2 ← State.get[JobDescriptorTable]
    } yield s2
  }

}

