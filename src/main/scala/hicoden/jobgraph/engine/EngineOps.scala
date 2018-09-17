package hicoden.jobgraph.engine

import hicoden.jobgraph.{Workflow, WorkflowOps, JobStates, WorkflowStates, WorkflowId}
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

import scala.language.postfixOps
import com.typesafe.scalalogging.Logger

/**
 * Responsibilities:
 * - Loads the hydrates the workflow and job configurations (in-memory and
 *   persistent storage)
 * - Loads the engine's configuration (in-memory)
 * - Interprets the DirectRunner's job status
 *
 * @author Raymond Tay
 * @verison 1.0
 */
trait EngineOps extends Concretizer with DatabaseOps {

  import cats._, data._, implicits._
  import hicoden.jobgraph.engine.runtime._

  object StepOps extends StepParser with StepLoader
  object WfOps extends WfParser with WfLoader
  object EngineCfgParser extends EngineConfigParser

  val logger : Logger = Logger(classOf[EngineOps])

  //
  // NOTE: DO NOT MANIPULATE THE FOLLOWING ADTS DIRECTLY !!!
  //       Do use the State Monad functions, augment your own if you need to 
  //
  //

  /* Mapping of job configuration (indexed by ids) to their model */
  var JDT : JobDescriptors = _

  /* Mapping of workflow configuration (indexed by ids) to their model */
  var WFDT : WorkflowDescriptors = _


  /**
    * Sets the internal state of the Engine with the given job and workflow
    * configuration descriptors
    * @param 2-tuple (jobdescriptors, workflowdescriptors)
    * @param 2 state objects
    * @return Right((job descriptor state object, workflow descriptor state object))
    */
  def updateJDTNWFDTTableState : Reader[(JobDescriptors,WorkflowDescriptors), Either[_, (JobDescriptors, WorkflowDescriptors)]] =
    Reader{ (p: (JobDescriptors,WorkflowDescriptors)) ⇒
      p.bimap(l ⇒ setJDT(l.map), r ⇒ setWFDT(r.map)).bimap(_.runS(p._1).value, _.runS(p._2).value).asRight
    }

  /**
    * Updates the status of the workflow and associated jobs to the database
    * @param wfId
    * @param jobs
    * @param rollbackXa database transactor that would rollback when an error
    *                   is detected
    */
  def updateWorkflowNJobStatusToDatabase(workflowId: WorkflowId)(jobs: List[Job]) : Reader[Transactor[cats.effect.IO], Either[String,Int]] =
    Reader { (rollbackXa: Transactor[cats.effect.IO]) ⇒
      import doobie._
      import doobie.implicits._ // this is where the "quick" method comes into play
      import doobie.postgres._
      import doobie.postgres.implicits._
      import cats._
      import cats.data._
      import cats.effect.IO
      import cats.implicits._

      (updateWorkflowStatusToDatabase(WorkflowStates.forced_termination)(workflowId).run.attempt *> 
       updateJobStatusToDatabase(JobStates.forced_termination)(jobs).run.attempt
      ).transact(rollbackXa).unsafeRunSync.bimap(error ⇒ error.getMessage, identity)
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
  def validateJobOverrides : Reader[JobConfigOverrides, Option[List[Int]]] =
    Reader{ (overrides:JobConfigOverrides) ⇒
      if (JDT.map.isEmpty) none
      else {
        val incoming = Set(overrides.overrides.collect{ case c ⇒ c.id }:_*)
        val intersect = JDT.map.keySet & incoming

        if (intersect.isEmpty) none // absolutely nothing in common
        else if ((intersect & incoming) == intersect) incoming.toList.some // everything indicated is there
        else none // accounts for everything else
      }
    }

  // Set the [[JobDescriptorTable]] into the State object
  private
  def setJDT(datum: JobDescriptorTable) : State[JobDescriptors, Boolean] =
    for {
      x  ← State.get[JobDescriptors]
      _  ← State.modify((s: JobDescriptors) ⇒ s.copy(map = datum))
      x2 ← State.get[JobDescriptors]
    } yield {
      JDT = x2
      true
    }

  // Set the [[WorkflowDescriptorTable]] into the State object
  private
  def setWFDT(datum: WorkflowDescriptorTable) : State[WorkflowDescriptors, Boolean] =
    for {
      x  ← State.get[WorkflowDescriptors]
      _  ← State.modify((s: WorkflowDescriptors) ⇒ s.copy(map = datum))
      x2 ← State.get[WorkflowDescriptors]
    } yield {
      WFDT = x2
      true
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
      s  ← State.get[JobDescriptors]
      _  ← State.modify{(dt: JobDescriptors) ⇒ dt.copy(map = StepOps.hydrateJobConfigs(jobConfig :: Nil).runS(dt.map).value)}
      s2 ← State.get[JobDescriptors]
    } yield {
      JDT = s2
      true
    }
  }


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
    * Invoked iff when the commandline option "--initDb=no" (see
    * [[Engine.preStart]]) and jobgraph hydrates the configuration
    * data present in the database and returns it
    * @return 2-tuple where left is [[JobDescriptorTable]] and
    *         right is [[WorkflowDescriptorTable]].
    */
  def loadAllConfigTemplatesFromDatabase = {
    import doobie._
    import doobie.implicits._ // this is where the "quick" method comes into play
    import doobie.postgres._
    import doobie.postgres.implicits._
    import cats._
    import cats.data._
    import cats.effect.IO
    import cats.implicits._

    import Transactors.y._
    var jdt : JobDescriptorTable = collection.immutable.HashMap.empty[Int, JobConfig]
    var wfdt : WorkflowDescriptorTable = collection.immutable.HashMap.empty[Int, WorkflowConfig]

    // dont want leaky abstractions, so the nitty gritty stuff is done here.
    def setWorkflowTemplates = Reader{ (loadedWfConfigs: List[WorkflowConfig]) ⇒
      wfdt = WfOps.hydrateWorkflowConfigs(loadedWfConfigs).runS(wfdt).value
      ().pure[ConnectionIO]
    }
    def setJobTemplates = Reader{ (loadedJobConfigs: List[JobConfig]) ⇒ 
      jdt = StepOps.hydrateJobConfigs(loadedJobConfigs).runS(jdt).value
      ().pure[ConnectionIO]
    }

    ((selectAllWorkflowTemplates >>= setWorkflowTemplates.run) *>
     (selectAllJobTemplates      >>= setJobTemplates.run)).quick.unsafeRunSync

    (jdt, wfdt)
  }

  /**
    * Attempts to inserts data records into tables [[workflow_rt]] and [[job_rt]]
    * The transaction, if failed will be rolled back and maximum timeout for
    * this transaction is 30 secs.
    * @param jobOverrides either its something or nothing
    * @param workflow
    * @param jdt
    * @param wfdt
    * @return either None or Some(<number of rows>)
    */
  def insertNewWorkflowIntoDatabase : Reader[Workflow, Option[Int]] = Reader{ (workflow: Workflow) ⇒
    import doobie._
    import doobie.implicits._ // this is where the "quick" method comes into play
    import doobie.postgres._
    import doobie.postgres.implicits._
    import cats._
    import cats.data._
    import cats.effect.IO
    import cats.implicits._
    import scala.concurrent.duration._

    val rollbackUponErrorXa = Transactor.oops.set(Transactors.xa, HC.rollback)
    workflowRtOp(workflow).update.run.transact(rollbackUponErrorXa).unsafeRunTimed(30.seconds)
  }

  /**
    * Combinator function that updates both the inmemory state and database
    * state; upon a db failure it will propagate back the error to the [[Engine]]
    * Note: A maximum timeout of 10 seconds is configured - internal to this
    * function.
    * @param wfId
    * @param wfStatus
    * @param jobId
    * @param jobStatus
    * @return a Left(exception object) or a Right(some boolean value indicating
    * the success/failure of the in-memory update)
    */
  def updateWorkflowDbNInmemory(wfId: WorkflowId)(jobId: JobId) : Reader[JobStates.States, Either[Exception,Option[Boolean]]] =
    Reader { (jobStatus: JobStates.States) ⇒
      import doobie._
      import doobie.implicits._
      import Transactors.y._
      import scala.concurrent.duration._

      updateJobStatusRT(jobStatus)(jobId).update.quick.attemptSql.unsafeRunTimed(10.seconds).fold[Either[Exception, Option[Boolean]]]{Left(new Exception(s"Timeout occurred! Update to job_rt failed for job: $jobId of workflow: $wfId."))}{
        case Left(sqle) ⇒ Left(new Exception(s"Update to job_rt failed for job: $jobId or workflow: $wfId with error: $sqle"))
        case Right(_)   ⇒ WorkflowOps.updateWorkflow(wfId)(jobId)(jobStatus)
      }
    }

  /**
    * Build the SQL statements and bunch them up.
    * @param jdt
    * @return Update0 - it's doobie's representation of a sql statement
    */
  def fillDatabaseJobConfigs : State[JobDescriptors, Update0] = {
    import doobie.implicits._
    for {
      jdt ← State.get[JobDescriptors]
    } yield jdt.map.values.map(jobConfigOp(_)).reduce(_ ++ _).update
  }

  /**
    * Build the SQL statement and bunch them up
    * @param wfdt
    * @return Update0 - it's doobie's representation of a sql statement
    */
  def fillDatabaseWorkflowConfigs : State[WorkflowDescriptors, Update0] = {
    import doobie.implicits._
    for {
      wfdt ← State.get[WorkflowDescriptors]
    } yield wfdt.map.values.map(workflowConfigOp(_)).reduce(_ ++ _).update
  }

  /** 
    * Adds new workflow configuration to the database table [[workflow_template]]
    * @param wfConfig
    * @return sql object
    */
  def addNewWorkflowToDatabase : Reader[WorkflowConfig, Update0] = Reader{ (wfConfig: WorkflowConfig) ⇒
    import doobie.implicits._
    workflowConfigOp(wfConfig).update
  }

  /** 
    * Adds new job configuration to the database table [[job_template]]
    * @param jobConfig
    * @return number of database rows
    */
  def addNewJobToDatabase : Reader[JobConfig, Update0] = Reader{ (jobConfig: JobConfig) ⇒
    import doobie.implicits._
    jobConfigOp(jobConfig).update
  }

  /**
    * Updates the database table [[workflow_rt]] for the matching workflowId
    * to the passed-in state
    * @param wfStatus 
    * @param wfId
    * @return sql object
    */
  def updateWorkflowStatusToDatabase(wfStatus: WorkflowStates.States) : Reader[WorkflowId, Update0] = Reader{ (wfId: WorkflowId) ⇒
    import doobie.implicits._
    updateWorkflowStatusRT(wfStatus)(wfId).update
  }

  /**
    * Builds the sql object to update the database table [[job_rt]] for the
    * jobs to the passed-in state 
    * @param jobStatus
    * @param jobs
    * @return sql object
    */
  def updateJobStatusToDatabase(jobStatus: JobStates.States) : Reader[List[Job], Update0] = Reader{ (jobs: List[Job]) ⇒
    import doobie.implicits._
    jobs.map(job ⇒ updateJobStatusRT(jobStatus)(job.id)).reduce(_ ++ _).update
  }

  /**
    * Returns all workflows currently present in the system; pagination
    * mechanism would be implemented at a later stage.
    * @param wfdt
    * @return a container where each value is a [[WorkflowConfig]] object
    */
  def getAllWorkflows : State[WorkflowDescriptors, List[WorkflowConfig]] =
    for {
      s ← State.get[WorkflowDescriptors]
    } yield s.map.values.toList

  /**
    * Returns all job configurations currently present in the system; pagination
    * mechanism would be implemented at a later stage.
    * @param wfdt
    * @return a container where each value is a [[JobConfig]] object
    */
  def getAllJobs : State[JobDescriptors, List[JobConfig]] =
    for {
      s ← State.get[JobDescriptors]
    } yield s.map.values.toList

  /**
   * Attempt to load the workflow by indexing its index in the configuration
   * file that was loaded (remember, by default its [[application.conf]]) 
   * @param workflowIndex
   * @param jobOverrides
   * @param jdt
   * @param wfdt
   * @return Some((List of [[LNode]], List of [[LEdge]])) or none
   */
  def extractWorkflowConfigBy(workflowIndex: Int, jobOverrides: Option[JobConfigOverrides])(implicit jdt : JobDescriptors, wfdt: WorkflowDescriptors) : Option[(List[LNode[Job,JobId]], List[LEdge[Job,String]])]= {
    if (wfdt.map.contains(workflowIndex))
      for {
        (nodes, edges) ← reify(jdt.map)(wfdt.map(workflowIndex)).toOption
      } yield jobOverrides.fold((nodes, edges))(overrides ⇒ mergeN(nodes, overrides) >>= ((updatedJobs:List[Job]) => (List.empty[LNode[Job,JobId]], mergeE(edges, updatedJobs))))
    else none
  }

  // merges the targeted graph nodes with the appropriate job overrides
  private
  def mergeN(nodes: List[LNode[Job,JobId]], overrides: JobConfigOverrides) : (List[LNode[Job,JobId]], List[Job]) = {
    val m : Map[Int, List[JobOverrides]] = overrides.overrides.groupBy(_.id)
    val updatedNodes = collection.mutable.ListBuffer.empty[Job]
    (nodes.map(gNode ⇒ m.get(gNode.vertex.config.id).fold(gNode){
      overrde ⇒
        val n = merge(gNode.vertex, overrde.head)
        updatedNodes += n
        gNode.copy(vertex = n)
    }), updatedNodes.toList)
  }

  // merges the targeted graph nodes with the appropriate job overrides
  private
  def mergeE(edges: List[LEdge[Job,String]], overrides: List[Job]) : List[LEdge[Job,String]] = {
    val m = overrides.groupBy(_.id)
    edges.map(edge ⇒ m.get(edge.from.id).fold(edge)(job ⇒ edge.copy(from = job.head))).map(edge ⇒ m.get(edge.to.id).fold(edge)(job ⇒ edge.copy(to = job.head)))
  }

  // For the overridable fields of each [[Job]], we apply its corresponding
  // template's default value iff jobgraph does not see it.
  private
  def merge(j : Job, o: JobOverrides) : Job =  {
    def mergeCfg(l: JobConfig) =
      (o.description.fold(l.description)(identity).some,
       o.workdir.fold(l.workdir)(identity).some,
       o.sessionid.fold(l.sessionid)(identity).some,
       o.timeout.fold(l.timeout)(identity).some,
       o.runnerRunner.fold(l.runner.runner)(identity).some,
       o.runnerCliArgs.fold(l.runner.cliargs)(identity).some).mapN(
        (d: String, cwd: String, session: String, timeout: Int, runner: String, cliArgs: List[String]) ⇒ l.copy(description = d, workdir = cwd, sessionid = session, timeout = timeout, runner = l.runner.copy(runner = runner, cliargs = cliArgs)))

      mergeCfg(j.config).fold(j){cfg ⇒ j.copy(config = cfg)}
  }

  /**
    * Validate the job submission means that we check for a few things
    * - The job id must be distinct from the rest since it is the same id that
    *   is used to reference the job in the workflow DAG
    * - The dataflow must contain the right [[RunnerType]] [[ExecType]] pair
    *   else it is consider illegal
    * @param JDT
    * @param jobConfig
    * @return Some(jobConfig) else none
    */
  def validateJobSubmission(jdt: JobDescriptors) : Reader[JobConfig, Option[JobConfig]] =
    Reader{ (jCfg: JobConfig) ⇒
      if (isJobConfigPresent(jCfg).runA(jdt).value) none else {
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
  private def isJobConfigPresent = Reader{ (jCfg: JobConfig) ⇒
    for {
      s ← State.get[JobDescriptors]
    } yield s.map.contains(jCfg.id)
  }

  /**
    * Validate the workflow submission by checking whether it is valid (i.e.
    * all job nodes referenced must exist and there's no loop since we want
    * DAGs)
    * @param wfConfig
    * @return Some(<workflow index>) else none
    */
  def validateWorkflowSubmission(jdt: JobDescriptors, wfdt: WorkflowDescriptors) : Reader[WorkflowConfig, Option[WorkflowConfig]] =
    Reader{ (wfConfig: WorkflowConfig) ⇒
      if (isWorkflowIndexExisting(wfdt)(wfConfig)) none
      else
      reify(jdt.map)(wfConfig).fold(
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
  def isWorkflowIndexExisting(wfdt: WorkflowDescriptors) : Reader[WorkflowConfig, Boolean] =
    Reader{ (wfConfig: WorkflowConfig) ⇒
      if (wfdt.map.contains(wfConfig.id)) true else false
    }

  /**
    * State function that adds the workflow configuration to the workflow
    * descriptor table and returns this updated version.
    * @param wfConfig
    * @param wfdt
    * @return updated Workflow Descriptor Table
    */
  def addNewWorkflowToWFDT = Reader{ (wfConfig: WorkflowConfig) ⇒
    for {
      s  ← State.get[WorkflowDescriptors]
      _  ← State.modify{(dt: WorkflowDescriptors) ⇒
             dt.copy(map = WfOps.hydrateWorkflowConfigs(wfConfig :: Nil).runS(dt.map).value)
           }
      s2 ← State.get[WorkflowDescriptors]
    } yield {
      WFDT = s2
      true
    }
  }

}

