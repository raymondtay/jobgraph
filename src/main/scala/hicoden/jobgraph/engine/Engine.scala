package hicoden.jobgraph.engine

import hicoden.jobgraph._
import akka.actor._
import akka.pattern._
import akka.routing.{ ActorRefRoutee, RoundRobinRoutingLogic, Router }
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import scala.language.{higherKinds, postfixOps}
import scala.util._
import scala.concurrent.duration._
import hicoden.jobgraph.engine.persistence.Transactors
import hicoden.jobgraph.engine.runtime.jobOverridesDecoder
import hicoden.jobgraph.configuration.engine.HOCONValidation
import hicoden.jobgraph.configuration.engine.model.{MesosConfig, JobgraphConfig}
import hicoden.jobgraph.configuration.step.model.JobConfig
import hicoden.jobgraph.configuration.workflow.model.{WorkflowConfig, JobConfigOverrides, JobOverrides}
import hicoden.jobgraph.configuration.step.JobDescriptorTable
import hicoden.jobgraph.configuration.workflow.WorkflowDescriptorTable
import hicoden.jobgraph.fsm.{JobFSM, StartRun, StopRun, MonitorRun}
import java.util.UUID

//
// Engine Messages
//
// Purpose
// --------
// (a) Semantically speaking, once a workflow starts then it should continue
//     its natural course of execution w/o no intervention from the Engine; the
//     only thing left to do is to start it via [[StartWorkflow]] and stop is via
//     [[StopWorkflow]]; any updates received for the workflow is via
//     [[UpdateWorkflow]]. You have the option of overriding default options of
//     any job and workflow when you start it via
//     [[ValidateWorkflowJobOverrides]].
//
// (b) When any step of the workflow completes normally or abnormally, the job would
//     signal (via [[UpdateWorkflow]]) the engine Engine would update its internal state
//     and decide what to do next.
//
// (c) The engine can respond a query of the workflow which returns a JSON
//     response illustrating the state of the entire workflow via
//     [[WorkflowRuntimeReport]]
//
// (d) The engine can respond to queries of all jobs and workflows via
//     [[JobListing]] and [[WorkflowListing]] respectively.
//
// (e) Apache beam jobs (running using DataflowRunner or DirectRunner) would
//     need to send a ACK or update back to the Engine so that it knows how to
//     proceed automatically.
//
// (f) Engine allows the online submission of new jobs and new workflows (which
//     is stored in the database upon successful validation) via
//     [[ValidateJobSubmission]] and [[ValidateWorkflowSubmission]] and they can
//     subsequently be triggered via [[StartWorkflow]]

case class StartWorkflow(jobOverrides: Option[io.circe.Json], workflowId: Int)
case class StopWorkflow(workflowId: WorkflowId)
case class UpdateWorkflow(workflowId : WorkflowId, jobId: JobId, signal: JobStates.States)
case class SuperviseJob(workflowId: WorkflowId, jobId: JobId, googleDataflowId: String)
case class ValidateWorkflowSubmission(wfConfig : WorkflowConfig)
case class ValidateJobSubmission(jobConfig : JobConfig)
case class ValidateWorkflowJobOverrides(payload: io.circe.Json)
case object WorkflowListing
case object JobListing
case class WorkflowRuntimeReport(workflowId: WorkflowId)


class Engine(initDb: Option[Boolean] = None,jobNamespaces: List[String], workflowNamespaces: List[String]) extends Actor with ActorLogging with EngineStateOps2 with EngineOps {
  import cats._, data._, implicits._
  import cats.effect.IO
  import cats.free._
  import WorkflowOps._
  import doobie._, doobie.implicits._

  private[this]
  var prohibiters = {
    val routees = Vector.fill(8) {
      val r = context.actorOf(Props[DataflowJobTerminator])
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }
  private[this] var mesosConfig : Option[MesosConfig] = none
  private[this] var jobgraphConfig : Option[JobgraphConfig] = none

  /**
    * When the Engine starts up, it would attempt to load the job and workflow
    * definitions (housed in their respective configuration files i.e.
    * application.conf) and inject the same data into their respective
    * configuration tables (see [[workflow_template]] and [[job_template]]) -
    * You should ever do this just once - treat it as a RESET.
    * When passed the command line options
    * "--initDb=yes" we basically re-create the data information as in the
    * configuration files but if this commandline option is omitted or carries
    * the value 'no' then it is meant to lift the configuration from the
    * database.
    */
  override def preStart() = {
    ACTIVE_WORKFLOWS     = ActiveWorkflows(collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]])
    FAILED_WORKFLOWS     = FailedWorkflows(collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]])
    WORKERS_TO_WF_LOOKUP = WorkersToWorkflow(collection.mutable.Map.empty[ActorPath, WorkflowId])
    ACTIVE_DATAFLOW_JOBS = ActiveGoogleDataflow(collection.mutable.Map.empty[GoogleDataflowId, WorkflowId])
    JDT                  = JobDescriptors(collection.immutable.HashMap.empty[Int, JobConfig])
    WFDT                 = WorkflowDescriptors(collection.immutable.HashMap.empty[Int, WorkflowConfig])
    if (!jobNamespaces.isEmpty && !workflowNamespaces.isEmpty) init()
  }

  def init() = {
    updateJDTNWFDTTableState(prepareDescriptorTables(jobNamespaces, workflowNamespaces).bimap(l ⇒ JDT.copy(map = l), r ⇒ WFDT.copy(map = r)))

    initDb.fold(overrideJobNWorkflowDescriptorsFromDatabase){ _ ⇒
      import Transactors.y._
      (deleteAllJobRuntimeRecords.run.attempt                   *>
       deleteAllWorkflowRuntimeRecords.run.attempt              *>
       deleteAllWorkflowTemplates.run.attempt                   *>
       deleteAllJobTemplates.run.attempt                        *>
       fillDatabaseWorkflowConfigs.runA(WFDT).value.run.attempt *>
       fillDatabaseJobConfigs.runA(JDT).value.run.attempt        ).quick.unsafeRunSync
    }

    mesosConfig    = loadMesosConfig.fold(whenUnableToLoadConfig("Mesos")(_), _.some)
    jobgraphConfig = loadEngineConfig.fold(whenUnableToLoadConfig("Engine")(_), _.some)
  }

  private def overrideJobNWorkflowDescriptorsFromDatabase {
    updateJDTNWFDTTableState(loadAllConfigTemplatesFromDatabase.bimap(l ⇒ JDT.copy(map = l), r ⇒ WFDT.copy(map = r)))
  }

  private
  def whenUnableToLoadConfig(namespace: String) = Reader{ (errors: NonEmptyList[HOCONValidation]) ⇒
    logger.warn(s"Unable to load $namespace Config; not going to use Apache Mesos: details $errors")
    None
  }

  /**
    * HOF where we extract the workflow configuration (referenced by the index
    * `workflowId`) and if we don't discover it we do (a); else we construct
    * the workflow and insert into the database and if fail we do (b) ;finally
    * the workflow needs workers to run and if we fail in that we do (c)
    * otherwise we will return runtime workflow id (which is a UUID-like tag)
    * (a) "No such id"
    * (b) "Database operation failed, unable to start workflow"
    *
    * @param jobOverrides
    * @param workflowId
    * @return the UUID workflow id or (a) / (b) or (c)
    */
  def attemptStartWorkflow(jobOverrides : Option[JobConfigOverrides]) = Reader{ (workflowId: Int) ⇒
    extractWorkflowConfigBy(workflowId, jobOverrides)(JDT, WFDT).fold{
      logger.error(s"[Engine][StartWorkflow] The workflow-id giving: $workflowId does not exist in the system")
      "No such id"
      }{
        (nodeEdges) ⇒
          val jobGraph = createWf(WFDT.map.get(workflowId), nodeEdges._1)(nodeEdges._2)
          insertNewWorkflowIntoDatabase(jobGraph).fold(s"Database operation failed, unable to start workflow: [$workflowId]"){ totalRowsInserted ⇒
            logger.debug("[Engine] Received a job graph id:{}", jobGraph.id)

            startWorkflow(jobGraph.id).fold(Monad[Id].pure(Set.empty[(akka.actor.ActorRef, Job)]))(Monad[Id].pure(createWorkers(_))) >>= ((workers: Set[(ActorRef,Job)]) ⇒ {
              activateWorkers(jobGraph.id)(workers)
              logger.info(s"[Engine] Attempted to activate workers for workflow: [${jobGraph.id}]")
              Monad[Id].pure(workers)
            }) >>= ((workers: Set[(ActorRef,Job)]) ⇒ {
              jobGraph.status = WorkflowStates.started
              updateWorkflowStatusToDatabase(jobGraph.status)(jobGraph.id).run.transact(Transactors.xa).unsafeRunSync
              addToLookup(jobGraph.id)(workers).runS(WORKERS_TO_WF_LOOKUP).value
              addToActive(jobGraph.id)(workers).runS(ACTIVE_WORKFLOWS).value
              logger.info("[Engine] Started a job graph")
            })

            jobGraph.id.toString
        }
    }
  }

  override def preRestart(cause: Throwable, msg: Option[Any]) {}

  def receive : PartialFunction[Any, Unit] = {

    case StartWorkflow(payload, workflowId) ⇒
      import io.circe.generic.auto._, io.circe.syntax._

      if (payload.isEmpty) {
        sender() ! attemptStartWorkflow(None)(workflowId)
      } else
        payload.get.as[JobConfigOverrides].bimap(decodingFailure ⇒ None, identity).toOption.fold{
          logger.error(s"[Engine] Starting of workflow index: [$workflowId] has failed due to invalid job overrides - this shouldn't happen.")
          sender() ! s"Unable to start workflow: [$workflowId] because of json decoding failure"
        }{ jobOverrides ⇒ sender() ! attemptStartWorkflow(jobOverrides.some)(workflowId) }

    // Updating the workflow effectively means we do a few things:
    // (a) Update the job's state for the workflow and if something happens
    //     during the updates, this failed workflow will be pushed to an inactive
    //     workflow.
    // (b) From that current job, we shall be searching for the next few jobs
    //     to start.
    case UpdateWorkflow(wfId, jobId, signal) ⇒
      logger.info(s"[Engine][UpdateWorkflow] Going to update wf:$wfId, job:$jobId for signal: $signal")
      updateWorkflowDbNInmemory(wfId)(jobId)(signal).bimap(
        (error: Exception) ⇒ {
          logFailure.run(error)
          if (dropWorkflowFromActiveToFailedBy(wfId))
            logger.info(s"[Engine][UpdateWorkflow] Dropped workflow: [$wfId] from 'active' to 'failed'")
          else
            logger.info(s"[Engine][UpdateWorkflow] Unable to drop workflow: [$wfId] from 'active' to 'failed'")
        },
        (status: Option[Boolean]) ⇒ {
          for {
            jobs        ← EitherT(discoverNext(wfId)(jobId))
            toTerminate ← EitherT(isJobToEuthanised(signal))
            _           ← EitherT(deactivateJobWorkerFromActive(wfId, toTerminate)(jobId))
          } yield {
            if(!jobs.isEmpty) {
              logger.info(s"[Engine][UpdateWorkflow] Going to instantiate workers for this batch : $jobs.")

              startJobs(wfId)(jobs) *>
              logger.info(s"[Engine][UpdateWorkflow] Successfully started new workers for jobs : $jobs.") *>
              updateActiveNLookupTableState(ACTIVE_WORKFLOWS, WORKERS_TO_WF_LOOKUP)

            } else {
              if (isWorkflowCompleted(wfId))
                updateWorkflowStatusToDatabase(WorkflowStates.finished)(wfId).run.transact(Transactors.xa).unsafeRunSync
              else if (isWorkflowForcedStop(wfId))
                updateWorkflowStatusToDatabase(WorkflowStates.forced_termination)(wfId).run.transact(Transactors.xa).unsafeRunSync
              logger.info(s"[Engine][UpdateWorkflow] Nothing to do for wf: $wfId")
            }
          }
        }
      )

    // Lookup in the active workflows info and try to find the [[wfId]] and
    // [[jobId]] and if found, we trigger the monitoring to happen. As a
    // side-effect, we create the mapping wfId -> google-dataflow-id
    //
    case SuperviseJob(wfId, jobId, googleDataflowId) ⇒
      logger.info(s"[Engine][SuperviseJob] Received $wfId $jobId $googleDataflowId")
      Either.cond(
        isWorkflowInActiveWorkflows(wfId).runA(ACTIVE_WORKFLOWS).value,
        lookupWorkerFromActive(wfId)(jobId).runA(ACTIVE_WORKFLOWS).value.fold(logger.warn(s"[Engine][SuperviseJob] Did not locate the job: $jobId")){
          (p: (ActorRef, Job)) ⇒
            p._1 ! MonitorRun(wfId, jobId, self, googleDataflowId)
            bindDataflowToWorkflow(wfId)(googleDataflowId).runA(ACTIVE_DATAFLOW_JOBS).value
            logger.info(s"[Engine][SuperviseJob] Engine will start supervising Google dataflow job: $googleDataflowId")
        },
        logger.error(s"[Engine][SuperviseJob] Did not see either workflow:$wfId , job:$jobId")
      )

    // De-activation means that we update the state (both in-memory and
    // database) of the workflow to 'forced_termination' and the workers
    // will be shutdown.
    case StopWorkflow(wfId) ⇒
      Either.cond(isWorkflowInActiveWorkflows(wfId).runA(ACTIVE_WORKFLOWS).value,
      {
        import doobie.postgres._
        val rollbackXa = doobie.util.transactor.Transactor.oops.set(Transactors.xa, HC.rollback)

        for {
          ns   ← EitherT(stopWorkflow(wfId))
          wfs  ← EitherT(deactivateWorkflowWorkers(wfId))
          dfs  ← EitherT(cancelGoogleDataflowJobs(wfId))
        } yield {
          (updateWorkflowStatusToDatabase(WorkflowStates.forced_termination)(wfId).run.attempt *> 
           updateJobStatusToDatabase(JobStates.forced_termination)(ns).run.attempt
          ).transact(rollbackXa).unsafeRunSync
          logger.info("[Engine][StopWorkflow] {} nodes were updated for workflow id:{} and should be stopped.", ns.size, wfId)
        }
      },
        logger.error("[Engine][StopWorkflow] Attempting to stop a workflow id:{} that does not exist!", wfId)
      )

    case ValidateWorkflowSubmission(cfg) ⇒
      validateWorkflowSubmission(JDT, WFDT)(cfg).fold{
        logger.error(s"[Engine][Internal] Unable to validate the workflow submission : ${cfg}")
        sender() ! none
        }{
        (workflowConfig: WorkflowConfig) ⇒
          import doobie.postgres._
          import Transactors.y._
          val current = WFDT.map.size

          addNewWorkflowToDatabase(workflowConfig).run.attemptSomeSqlState {
            case sqlstate.class23.UNIQUE_VIOLATION ⇒
              logger.error(s"Database unique violation for workflow: [${workflowConfig}]")
              sender() ! None
          }.map(r ⇒
            if (r.isRight) {
              addNewWorkflowToWFDT(workflowConfig).runS(WFDT).value
              val newSize = WFDT.map.size
              if ((newSize - current) == 0)
                logger.info(s"[Engine] workflow has been added to repository: $current -> $newSize")
              else logger.info(s"[Engine] workflow has not been added to repository.")

              sender() ! Some(cfg.id)
            } else {
              logger.error(s"[Engine] caught some unhandled exception: $r")
              sender() ! None
            }
          ).quick.unsafeRunSync
        }

    case ValidateJobSubmission(cfg) ⇒
      validateJobSubmission(JDT)(cfg).fold{
        logger.error(s"[Engine][Internal] Unable to validate the job submission: ${cfg}")
        sender() ! none
        }{
          (jobCfg: JobConfig) ⇒
            import doobie.postgres._
            import Transactors.y._
            val current = JDT.map.size

            addNewJobToDatabase(jobCfg).run.attemptSomeSqlState{
              case sqlstate.class23.UNIQUE_VIOLATION ⇒
                logger.error("Unable to insert record into 'job_template' because of UNIQUE_VIOLATION")
                sender() ! None
            }.map(r ⇒
              if (r.isRight) {
                addNewJob(jobCfg).runS(JDT).value
                val newSize = JDT.map.size
                if ((newSize - current) == 0)
                  logger.info(s"[Engine] job has been added to repository: $current -> $newSize")
                else logger.info(s"[Engine] job has not been added to repository")

                sender() ! Some(jobCfg.id)
              } else {
                logger.error(s"[Engine] caught some unhandled exception: $r")
                sender() ! None
              }
            ).quick.unsafeRunSync
        }

    case WorkflowRuntimeReport(workflowId) ⇒ sender() ! getWorkflowStatus(workflowId)

    case WorkflowListing ⇒ sender() ! getAllWorkflows.runA(WFDT).value

    case JobListing ⇒ sender() ! getAllJobs.runA(JDT).value

    case ValidateWorkflowJobOverrides(payload) ⇒
      import io.circe.generic.auto._, io.circe.syntax._
      val validationResult = payload.as[JobConfigOverrides].bimap(decodingFailure ⇒ false, data ⇒ validateJobOverrides(data).fold(false)(_ ⇒ true)).toOption
      sender() ! validationResult

    case Terminated(child) ⇒
      val wfId = removeFromLookup(child).runA(WORKERS_TO_WF_LOOKUP).value
      logger.debug("[Engine][Internal] The job: {} has terminated for workflow: {}.", child, wfId)
  }

  /**
    * Determine if the job is eligible for termination by interpreting the
    * states.
    * @param signal JobStates
    * @return Left(<some message indicating no>) or Right(a tautology
    * indicating yes)
    */
  def isJobToEuthanised = Reader { (signal: JobStates.States) ⇒
    Either.cond(Set(JobStates.finished,
                    JobStates.forced_termination,
                    JobStates.failed,
                    JobStates.unknown).contains(signal),
      true,
      s"JobState:[$signal] is not to be euthanised."
    )
  }

  /**
    * Updates each job of the workflow to start, creates workers for each job
    * and adds that mapping to the active storage. If there is an error during
    * the update, we return the original state w/o modification.
    * @param wfId
    * @param jobs the set of jobs we shall be creating workers for
    * @return the updated active storage
    */
  def startJobs(wfId: WorkflowId) : Reader[Vector[Job], Either[(ActiveWorkflows, WorkersToWorkflow), (ActiveWorkflows, WorkersToWorkflow)]] =
    Reader{ (jobs: Vector[Job]) ⇒
      val startedNodes : Either[Throwable, Vector[Option[Boolean]]] = jobs.map(job ⇒ updateWorkflow(wfId)(job.id)(JobStates.start)).sequence
      startedNodes.bimap(
        (err: Throwable ) ⇒ (ACTIVE_WORKFLOWS, WORKERS_TO_WF_LOOKUP),
        (ys: Vector[Option[Boolean]]) ⇒ {
          createWorkers(Set(jobs: _*)) >>= ((workers:Set[(ActorRef,Job)]) ⇒ {
            activateWorkers(wfId)(workers)
            Monad[Id].pure(workers)
          }) >>= ((workers: Set[(ActorRef, Job)]) ⇒ {
            addToActive(wfId)(workers).runS(ACTIVE_WORKFLOWS).value
            Monad[Id].pure(workers)
          }) >>= ((workers: Set[(ActorRef, Job)]) ⇒ {
            addToLookup(wfId)(workers).runS(WORKERS_TO_WF_LOOKUP).value
            Monad[Id].pure(workers)
          })
          (ACTIVE_WORKFLOWS, WORKERS_TO_WF_LOOKUP)
        }
      )
    }

  /**
    * General function invoked when error encountered
    * @param t - [[java.lang.Throwable]] object representing errors
    * @return nothing - with the side effect that logs are pumped
    */
  def logFailure : Reader[Exception, Unit] = Reader{(t: Exception) ⇒
    logger.error("[logFailure] Error encountered with details")
    logger.debug(s"[logFailure] Stack Trace: ${t.getStackTrace.map(stackElement ⇒ logger.error(stackElement.toString))}")
  }

  /**
    * Fire the message to kick start
    * @param wfId - workflow id
    * @param actors - basically a set of (k,v) pairs where key is the actor
    * responsible for carrying out the "job"
    * @return nothing - no side-effects and this actor's event processing will
    * handle it
    */
  def activateWorkers(wfId: WorkflowId) : Reader[Set[(ActorRef, Job)], Set[Unit]] = Reader { (actors: Set[(ActorRef, Job)]) ⇒
    actors.map(actor ⇒ actor._1 ! StartRun(wfId, actor._2, self, mesosConfig, jobgraphConfig))
  }

  /**
    * Attempts to discover the FSM(s) associated with the given workflow id and
    * issue the [[StopRun]] command
    * @param wfId - workflow id
    * @return a Left which indicates a error condition or a Right which indicates success and state is returned.
    */
  def deactivateWorkflowWorkers : Reader[WorkflowId, Either[String, Boolean]] =
    Reader { (wfId: WorkflowId) ⇒
      Either.cond(
        isWorkflowInActiveWorkflows(wfId).runA(ACTIVE_WORKFLOWS).value,
        lookupWorkersFromActive(wfId).runA(ACTIVE_WORKFLOWS).value.fold(false)((workers: Map[ActorRef,Job]) ⇒ {
          workers.map(actor ⇒ actor._1 ! StopRun)
          removeActiveWorkflowsBy(wfId).runA(ACTIVE_WORKFLOWS).value.fold(false)(_ ⇒ true)
        })
        ,
        s"[DeactivateWorkers] Did not discover workflow $wfId in the internal state."
      )
    }

  /**
    * Attempts to discover the FSM(s) associated with the given workflow id and
    * job id; issue the [[StopRun]] command
    * @param wfId - workflow id
    * @param toTerminate - a truth value indicating whether to do it.
    * @param jobId - job id
    * @return a Left which indicates a error condition or a Right which indicates success and state is returned.
    */
  def deactivateJobWorkerFromActive(wfId: WorkflowId, toTerminate : Boolean) : Reader[JobId, Either[String, Boolean]] =
    Reader { (jobId: JobId) ⇒
      Either.cond(
        toTerminate && isWorkflowInActiveWorkflows(wfId).runA(ACTIVE_WORKFLOWS).value,
        {
          lookupWorkersFromActive(wfId).runA(ACTIVE_WORKFLOWS).value.fold(false)((workers: Map[ActorRef,Job]) ⇒ {
            workers.filter((p: (ActorRef,Job)) ⇒ p._2.id equals jobId).map(p ⇒ p._1 ! StopRun)
            removeJobFromActive(wfId)(jobId).runA(ACTIVE_WORKFLOWS).value
          })
        },
        s"[Engine][deactivateJobWorkerFromActive] Either jobgraph did not discover workflow $wfId in the internal state OR jobgraph is not allowed to terminate."
      )
    }

  /**
    * Attempts to cancel the google dataflow jobs by issuing a call to Google.
    * @param wfId - workflow id
    * @param xs - state data
    * @return a Left which indicates a error condition or a Right which indicates success and state is returned.
    */
  def cancelGoogleDataflowJobs : Reader[WorkflowId, Either[String, Boolean]] =
    Reader { (wfId: WorkflowId) ⇒
      val googleJobs = lookupDataflowBindings(wfId).runA(ACTIVE_DATAFLOW_JOBS).value
      Either.cond(
        !googleJobs.isEmpty,
        {
          prohibiters.route(WhatToStop(googleJobs), sender())
          removeFromDataflowBindings(wfId).runA(ACTIVE_DATAFLOW_JOBS).value
        },
        s"[Engine][cancelGoogleDataflowJobs] Did not discover workflow $wfId in the internal state."
      )
    }

  /**
    * Basically creates the Actor and associates its to the job; take note that
    * the engine watches over the lifecycle of the worker too. All this is done
    * via Yoneda transformation.
    * @param jobs - container of jobs to associate
    * @return set - a set of (k,v) pairs where the actor is associated with the
    * job
    */
  def createWorkers : Reader[Set[Job], Set[(ActorRef, Job)]] = Reader{ (jobs: Set[Job]) ⇒
    Set( Yoneda(jobs.toList).map( job ⇒ (lift[Id].run(createWorker(job.config)(context)) >>= (worker ⇒ (watchWorker(worker, job)(context))))).run : _*)
  }

  /**
    * Creates the asynchronous worker (associated with it is each job's restart
    * strategy with a exponential back-off option)
    * @param ctx this actor's context
    * @return an actor of type [[JobFSM]]
    */
  def createWorker(jobConfig: JobConfig) =
    Reader{(ctx: ActorContext) ⇒ ctx.actorOf(backoffOnFailureStrategy(Props(classOf[JobFSM], jobConfig.timeout), s"JobFSM@${java.util.UUID.randomUUID}", jobConfig))}

  // Plain'ol lifting
  def lift[A[_] : Monad] = Reader{ (actor: ActorRef) ⇒ Monad[A].pure(actor) }

  /**
    * Engine actor would look out for the child actor and returns the
    * association to the job 
    * @param worker Reference to the created actor
    * @param job the association of the worker to the current job
    * @param ctx this actor's context
    * @return a 2-tuple where the worker is associated with
    */
  def watchWorker(worker: ActorRef, job: Job) =
    Reader{ (ctx: ActorContext) ⇒ 
             ctx.watch(worker)
             (worker,job) }

}

object Engine extends App with JobCallbacks with WorkflowWebServices with JobWebServices {
  import akka.pattern.ask
  import akka.http.scaladsl.server.Directives._
  import scala.concurrent._, duration._

  // Load all the properties of the engine e.g. size of thread pool, timeouts
  // for the various parts of the engine while processing.

  val waitTimeForCleanup = 4000
  val waitTimeForAsyncProcessing = 30000

  implicit val actorSystem = ActorSystem("EngineSystem")
  implicit val executionContext = actorSystem.dispatcher
  implicit val actorMaterializer = ActorMaterializer()

  // terminating condition
  private def whenCliOptUnavailable = {
    println("An illegal option was given, and we are exiting because we dont know how to handle this.")    
    Actor.noSender
  }

  // Parse the command line opts given
  val engine =
    EngineCliOptsParser.parseCommandlineArgs(args).
      fold(whenCliOptUnavailable)(cliOpt ⇒ actorSystem.actorOf(Props(classOf[Engine], cliOpt.initDb, "jobs":: Nil, "workflows" :: Nil), "Engine"))

  if (engine == Actor.noSender) {
    println("Exiting Jobgraph engine now.")
    System.exit(-1)
  }

  // Bind the engine to serve ReST
  val bindingFuture = Http().bindAndHandle(JobCallbackRoutes ~ WorkflowWebServicesRoutes ~ JobWebServicesRoutes, "0.0.0.0")
  println(s"Server online at http://0.0.0.0:9000/\nPress RETURN to stop...")
  scala.io.StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ ⇒ actorSystem.terminate()) // and shutdown when done

  Thread.sleep(waitTimeForAsyncProcessing)

  Thread.sleep(waitTimeForCleanup)

}

