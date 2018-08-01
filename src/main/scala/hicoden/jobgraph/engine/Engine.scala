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
//     [[StopWorkflow]]
//
// (b) When any step of the workflow completes, the job would signal (via
//     [[UpdateWorkflow]]) the engine Engine would update its internal state
//     and decide what to do next.
//

case class StartWorkflow(workflowId: Int)
case class StopWorkflow(workflowId: WorkflowId)
case class UpdateWorkflow(workflowId : WorkflowId, jobId: JobId, signal: JobStates.States)
case class SuperviseJob(workflowId: WorkflowId, jobId: JobId, googleDataflowId: String)
case class ValidateWorkflowSubmission(wfConfig : WorkflowConfig)
case class ValidateJobSubmission(jobConfig : JobConfig)
case class ValidateWorkflowJobOverrides(payload: io.circe.Json)
case object WorkflowListing
case object JobListing
case class WorkflowRuntimeReport(workflowId: WorkflowId)


//
// Engine should perform the following:
// (a) Start the Workflow (which is essentially updating its in-memory graph structure and starting a FSM to execute the jobs)
// (b) Receive signals from the FSM actors about the execution status
// (c) Update its internal structure and decides whether to push the execution to the next step(s) because there will be wait-times at certain control
//     points - as decided by the digraph (take note that it can be a multi-graph)
//
class Engine(initDb: Option[Boolean] = None,jobNamespaces: List[String], workflowNamespaces: List[String]) extends Actor with ActorLogging with EngineStateOps with EngineOps {
  import cats._, data._, implicits._
  import cats.effect.IO
  import cats.free._
  import WorkflowOps._
  import doobie._, doobie.implicits._

  // TODO:
  // (a) ADTs should be accessible from any node in the cluster that's right we
  //     are talking about peer-peer actor clusters. Coming up soon !
  //
  private[this] var activeWorkflows   = collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]]
  private[this] var activeDataflows   = collection.mutable.Map.empty[GoogleDataflowId, WorkflowId]
  private[this] var workersToWfLookup = collection.mutable.Map.empty[ActorPath, WorkflowId]
  private[this] var failedWorkflows   = collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]]
  private[this] var jdt : JobDescriptorTable = collection.immutable.HashMap.empty[Int, JobConfig]
  private[this] var wfdt : WorkflowDescriptorTable  = collection.immutable.HashMap.empty[Int, WorkflowConfig]
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
  override def preStart() = { if (!jobNamespaces.isEmpty && !workflowNamespaces.isEmpty) init() }

  def init() = {
    for { (l,r) <- prepareDescriptorTables(jobNamespaces, workflowNamespaces) :: Nil } { jdt = l; wfdt = r }

    initDb.fold{ for { (l, r) ← loadAllConfigTemplatesFromDatabase :: Nil} {jdt = l; wfdt = r} }{ _ ⇒
      import Transactors.y._
      (deleteAllWorkflowTemplates.run.attempt        *>
       deleteAllJobTemplates.run.attempt             *>
       fillDatabaseWorkflowConfigs(wfdt).run.attempt *>
       fillDatabaseJobConfigs(jdt).run.attempt        ).quick.unsafeRunSync
    }

    mesosConfig    = loadMesosConfig.fold(whenUnableToLoadConfig("Mesos")(_), _.some)
    jobgraphConfig = loadEngineConfig.fold(whenUnableToLoadConfig("Engine")(_), _.some)
  }

  private def whenUnableToLoadConfig(namespace: String) = Reader{ (errors: NonEmptyList[HOCONValidation]) ⇒
    logger.warn(s"Unable to load $namespace Config; not going to use Apache Mesos: details $errors")
    None
  }

  override def preRestart(cause: Throwable, msg: Option[Any]) {}

  def receive : PartialFunction[Any, Unit] = {

    case StartWorkflow(workflowId) ⇒
      sender !
      extractWorkflowConfigBy(workflowId)(jdt, wfdt).fold{
        logger.error(s"[Engine][StartWorkflow] The workflow-id giving: $workflowId does not exist in the system")
        "No such id"
        }{
          (nodeEdges) ⇒
            val jobGraph = createWf(wfdt.get(workflowId), nodeEdges._1)(nodeEdges._2)
            logger.debug("[Engine] Received a job graph id:{}", jobGraph.id)
            val workers = startWorkflow(jobGraph.id).fold(Set.empty[(ActorRef, Job)])(createWorkers(_))
            if (workers.isEmpty) {
              logger.error("""
                [Engine] We just started a workflow {} where there are no start nodes.
                [Engine] Workflow is {}
                """, jobGraph.id, jobGraph)
              jobGraph.id.toString
            } else {
              activateWorkers(jobGraph.id)(workers)
              workersToWfLookup = addToLookup(jobGraph.id)(workers).runS(workersToWfLookup).value
              activeWorkflows   = addToActive(jobGraph.id)(workers).runS(activeWorkflows).value
              logger.info("[Engine] Started a job graph")
              jobGraph.id.toString
            }
      }

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
          logFailure.run(error) >>
          dropWorkflowFromActive(activeWorkflows, failedWorkflows)(wfId).bimap(
            (errorMessage: String) ⇒ logger.error(s"[Engine][UpdateWorkflow] Error in updating workflow with message: $errorMessage ."),
            (pair: (Map[WorkflowId, Map[ActorRef, Job]], Map[WorkflowId, Map[ActorRef, Job]])) ⇒ {
              activeWorkflows = pair._1
              failedWorkflows = pair._2
            }
          )
        },
        (status: Option[Boolean]) ⇒ {
          for {
            jobs ← EitherT(discoverNext(wfId)(jobId))
          } yield {
            if(!jobs.isEmpty) {
              logger.info(s"[Engine][UpdateWorkflow] Going to instantiate workers for this batch : $jobs.")
              startJobs(wfId)(activeWorkflows, workersToWfLookup)(jobs) match {
                case Left((a, b)) ⇒
                  logger.error(s"[Engine][UpdateWorkflow] Error in starting new workers for jobs : $jobs.")
                  activeWorkflows = a; workersToWfLookup = b
                case Right((a, b)) ⇒
                  logger.info(s"[Engine][UpdateWorkflow] Successfully started new workers for jobs : $jobs.")
                  activeWorkflows = a; workersToWfLookup = b
              }
            } else logger.info(s"[Engine][UpdateWorkflow] Nothing to do for wf: $wfId")
          }
        }
      )

    // Lookup in the active workflows info and try to find the [[wfId]] and
    // [[jobId]] and if found, we trigger the monitoring to happen. As a
    // side-effect, we create the mapping wfId -> google-dataflow-id
    //
    case SuperviseJob(wfId, jobId, googleDataflowId) ⇒
      logger.info(s"[Engine][SuperviseJob] Received $wfId $jobId $googleDataflowId")
      activeWorkflows.contains(wfId) match {
        case true  ⇒
          lookupActive(wfId)(jobId).runA(activeWorkflows).value.fold(logger.warn(s"Did not locate the job: $jobId")){
            (p: (ActorRef, Job)) ⇒
              p._1 ! MonitorRun(wfId, jobId, self, googleDataflowId)
              activeDataflows = bindDataflowToWorkflow(wfId)(googleDataflowId).runS(activeDataflows).value
              logger.info(s"[Engine][SuperviseJob] Engine will start supervising Google dataflow job: $googleDataflowId")
          }
        case false ⇒ logger.error(s"Did not see either workflow:$wfId , job:$jobId")
      }


    // De-activation means that we update the state (both in-memory and
    // database) of the workflow to 'forced_termination' and the workers
    // will be shutdown.
    case StopWorkflow(wfId) ⇒
      Either.cond(activeWorkflows.contains(wfId),
      {
        import doobie.postgres._
        import Transactors.y._
        for {
          ns   ← EitherT(stopWorkflow(wfId))
          wfs  ← EitherT(deactivateWorkers(wfId)(activeWorkflows))
          dfs  ← EitherT(cancelGoogleDataflowJobs(wfId)(activeDataflows))
        } yield {
          (updateWorkflowStatusToDatabase(WorkflowStates.forced_termination)(wfId).run.attempt *> 
           updateJobStatusToDatabase(JobStates.forced_termination)(ns).run.attempt *>
           {activeWorkflows = wfs}.pure[ConnectionIO] *>
           {activeDataflows = dfs}.pure[ConnectionIO]
          ).quick.unsafeRunSync
          logger.info("[Engine][StopWorkflow] {} nodes were updated for workflow id:{} and should be stopped.", ns.size, wfId)
        }

      },
        logger.error("[Engine][StopWorkflow] Attempting to stop a workflow id:{} that does not exist!", wfId)
      )

    case ValidateWorkflowSubmission(cfg) ⇒ 
      validateWorkflowSubmission(jdt, wfdt)(cfg).fold{
        logger.error(s"[Engine][Internal] Unable to validate the workflow submission : ${cfg}")
        sender() ! none
        }{
        (workflowConfig: WorkflowConfig) ⇒
          import doobie.postgres._
          import Transactors.y._
          val current = wfdt.size

          addNewWorkflowToDatabase(workflowConfig).run.attemptSomeSqlState {
            case sqlstate.class23.UNIQUE_VIOLATION ⇒
              logger.error("Database unique violation!")
              sender() ! None
          }.map(r ⇒
            if (r.isRight) {
              wfdt = addNewWorkflow(workflowConfig).runS(wfdt).value
              val newSize = wfdt.size
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
      validateJobSubmission(jdt)(cfg).fold{
        logger.error(s"[Engine][Internal] Unable to validate the job submission: ${cfg}")
        sender() ! none
        }{
          (jobCfg: JobConfig) ⇒
            import doobie.postgres._
            import Transactors.y._
            val current = jdt.size

            addNewJobToDatabase(jobCfg).run.attemptSomeSqlState{
              case sqlstate.class23.UNIQUE_VIOLATION ⇒
                logger.error("Unable to insert record into 'job_template' because of UNIQUE_VIOLATION")
                sender() ! None
            }.map(r ⇒
              if (r.isRight) {
                jdt = addNewJob(jobCfg).runS(jdt).value
                val newSize = jdt.size
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

    case WorkflowListing ⇒ sender() ! getAllWorkflows.runA(wfdt).value

    case JobListing ⇒ sender() ! getAllJobs.runA(jdt).value

    case ValidateWorkflowJobOverrides(payload) ⇒
      import io.circe.generic.auto._, io.circe.syntax._
      val validationResult =
        payload.as[JobConfigOverrides].bimap(decodingFailure ⇒ false, data ⇒ validateJobOverrides(jdt)(data).fold(false)(_ ⇒ true)).toOption
      sender() ! validationResult

    case Terminated(child) ⇒
      val (xs, result) = removeFromLookup(child).run(workersToWfLookup).value
      workersToWfLookup = xs
      val workflowId : WorkflowId = result._1
      logger.debug("[Engine][Internal] The job: {} has terminated for workflow: {}.", child, workflowId)
  }

  /**
    * Updates each job of the workflow to start, creates workers for each job
    * and adds that mapping to the active storage. If there is an error during
    * the update, we return the original state w/o modification.
    * @param wfId
    * @param active the active storage at the time
    * @param jobs the set of jobs we shall be creating workers for
    * @return the update active storage
    */
  def startJobs(wfId: WorkflowId)(active: Map[WorkflowId, Map[ActorRef,Job]], lookup: Map[ActorPath, WorkflowId]) : Reader[Vector[Job], Either[(Map[WorkflowId, Map[ActorRef,Job]], Map[ActorPath, WorkflowId]), (Map[WorkflowId, Map[ActorRef, Job]], Map[ActorPath, WorkflowId])]] = Reader{ (jobs: Vector[Job]) ⇒
    val startedNodes : Either[Throwable, Vector[Option[Boolean]]] = jobs.map(job ⇒ updateWorkflow(wfId)(job.id)(JobStates.start)).sequence
    startedNodes.bimap(
      (err: Throwable ) ⇒ (active, lookup),
      (ys: Vector[Option[Boolean]]) ⇒ {
        val workers = createWorkers(Set(jobs:_*))
        activateWorkers(wfId)(workers)
        (
          addToActive(wfId)(workers).runS(active).value,
          addToLookup(wfId)(workers).runS(lookup).value
        )
      }
    )
  }
  /**
    * Attempts to find the workflow from the active storage, removes it and
    * places it to the failed storage
    * @param active
    * @param failed
    * @param wfId
    * @return a 2-tuple where (failed + wfId, active - wfId)
    */
  def dropWorkflowFromActive(active: Map[WorkflowId, Map[ActorRef, Job]], failed: Map[WorkflowId, Map[ActorRef, Job]]) = Reader{ (wfId: WorkflowId) ⇒
    Either.cond(
      active.contains(wfId),
      (failed + (wfId → active(wfId)), active - wfId),
      s"[Engine][dropWorkflowFromActive] Did not locate workflow in active storage, weird"
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
    * @param xs - state data
    * @return a Left which indicates a error condition or a Right which indicates success and state is returned.
    */
  def deactivateWorkers(wfId: WorkflowId) : Reader[WFA, Either[String, WFA]] = Reader { (workflows: WFA) ⇒
    Either.cond(
      workflows.contains(wfId),
      {
        workflows(wfId).map(actor ⇒ actor._1 ! StopRun)
        removeFromActive(wfId).runS(workflows).value
      },
      s"[DeactivateWorkers] Did not discover workflow $wfId in the internal state."
    )
  }

  /**
    * Attempts to cancel the google dataflow jobs by issuing a call to Google.
    * @param wfId - workflow id
    * @param xs - state data
    * @return a Left which indicates a error condition or a Right which indicates success and state is returned.
    */
  def cancelGoogleDataflowJobs(wfId: WorkflowId) : Reader[Map[GoogleDataflowId, WorkflowId], Either[String, Map[GoogleDataflowId, WorkflowId]]] =
    Reader { (dataflows: Map[GoogleDataflowId, WorkflowId]) ⇒
      val gJobs = lookupDataflowBindings(wfId).runA(dataflows).value
      Either.cond(
        !gJobs.isEmpty,
        {
          prohibiters.route(WhatToStop(gJobs), sender())
          removeFromDataflowBindings(wfId).runA(dataflows).value
        },
        s"[cancelGoogleDataflowJobs] Did not discover workflow $wfId in the internal state."
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
    Reader{(ctx: ActorContext) ⇒ ctx.actorOf(backoffOnFailureStrategy(Props(classOf[JobFSM]), s"JobFSM@${java.util.UUID.randomUUID}", jobConfig))}

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

  // start a job graph running
  // TODO: remove this in the next iteration
  // implicit val timeout = akka.util.Timeout(5 seconds)
  // val workflowId : WorkflowId = java.util.UUID.fromString(Await.result((engine ? StartWorkflow(0)).mapTo[String], timeout.duration))

  // Bind the engine to serve ReST
  val bindingFuture = Http().bindAndHandle(JobCallbackRoutes ~ WorkflowWebServicesRoutes ~ JobWebServicesRoutes, "0.0.0.0")
  println(s"Server online at http://0.0.0.0:9000/\nPress RETURN to stop...")
  scala.io.StdIn.readLine() // let it run until user presses return
  bindingFuture
    .flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ ⇒ actorSystem.terminate()) // and shutdown when done

  Thread.sleep(waitTimeForAsyncProcessing)

  // stops the workflow aka "forced termination" of the jobgraph
  //engine ! StopWorkflow(workflowId)

  Thread.sleep(waitTimeForCleanup)

}

