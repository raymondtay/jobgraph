package hicoden.jobgraph.engine

import hicoden.jobgraph._
import akka.actor._
import scala.language.{higherKinds, postfixOps}
import scala.util._
import scala.concurrent.duration._
import hicoden.jobgraph.{ConvergeGraph, ScatterGatherGraph}        // a Digraph scatter-gatther
import hicoden.jobgraph.fsm.{JobFSM, StartRun, StopRun}
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

case class StartWorkflow(jobgraph: Workflow)
case class StopWorkflow(wfId: WorkflowId)
case class UpdateWorkflow(workflowId : WorkflowId, jobId: JobId, signal: JobStates.States)

//
// Engine should perform the following:
// (a) Start the Workflow (which is essentially updating its in-memory graph structure and starting a FSM to execute the jobs)
// (b) Receive signals from the FSM actors about the execution status
// (c) Update its internal structure and decides whether to push the execution to the next step(s) because there will be wait-times at certain control
//     points - as decided by the digraph (take note that it can be a multi-graph)
//
class Engine extends Actor with ActorLogging with EngineStateOps {
  import cats._, data._, implicits._
  import cats.free._
  import WorkflowOps._

  // TODO:
  // (a) ADTs should be accessible from any node in the cluster
  //
  private[this] var activeWorkflows = collection.mutable.Map.empty[WorkflowId, Set[ActorRef]]
  private[this] var failedWorkflows = collection.mutable.Map.empty[WorkflowId, Set[ActorRef]]

  def receive : PartialFunction[Any, Unit] = {

    case StartWorkflow(jobGraph) ⇒
      logger.debug("[Engine] Received a job graph id:{}", jobGraph.id)
      val workers = startWorkflow(jobGraph.id).fold(Set.empty[(ActorRef, Job)])(createWorkers(_))
      if (workers.isEmpty) {
        logger.error("""
          [Engine] We just started a workflow {} where there are no start nodes.
          [Engine] Workflow is {}
          """, jobGraph.id, jobGraph)
      } else {
        activateWorkers(jobGraph.id)(workers)
        activeWorkflows = addToActive(jobGraph.id)(workers.map(_._1)).runS(activeWorkflows).value
        logger.info("[Engine] Started a job graph")
      }

    // Updating the workflow effectively means we do a few things:
    // (a) Update the job's state for the workflow and if something happens
    //     during the updates, this failed workflow will be pushed to an inactive
    //     workflow.
    // (b) From that current job, we shall be searching for the next few jobs
    //     to start.
    case UpdateWorkflow(wfId, jobId, signal) ⇒
      logger.info(s"[Engine][UpdateWorkflow] Going to update wf:$wfId, job:$jobId for signal: $signal")
      updateWorkflow(wfId)(jobId)(signal).bimap(
        (error: Throwable) ⇒ {
          logFailure.run(error) >>
          dropWorkflowFromActive(collection.immutable.Map(activeWorkflows.toList:_*),
                                 collection.immutable.Map(failedWorkflows.toList:_*))(wfId).bimap(
            (errorMessage: String) ⇒ logger.error(s"[Engine][UpdateWorkflow] Error in updating workflow with message: $errorMessage ."),
            (pair: (Map[WorkflowId, Set[ActorRef]], Map[WorkflowId, Set[ActorRef]])) ⇒ {
              activeWorkflows = collection.mutable.Map(pair._1.toList:_*)
              failedWorkflows = collection.mutable.Map(pair._2.toList:_*)
            }
          )
        },
        (status: Option[Boolean]) ⇒ {
          for {
            jobs ← EitherT(discoverNext(wfId)(jobId))
          } yield {
            if(!jobs.isEmpty) {
              logger.info(s"[Engine][UpdateWorkflow] Going to instantiate workers for this batch : $jobs.")
              startJobs(wfId)(collection.immutable.Map(activeWorkflows.toList:_*))(jobs) match {
                case Left(a) ⇒
                  logger.error(s"[Engine][UpdateWorkflow] Error in starting new workers for jobs : $jobs.")
                  activeWorkflows = collection.mutable.Map(a.toList: _*)
                case Right(b) ⇒
                  logger.info(s"[Engine][UpdateWorkflow] Successfully started new workers for jobs : $jobs.")
                  activeWorkflows = collection.mutable.Map(b.toList: _*)
              }
            } else logger.info(s"[Engine][UpdateWorkflow] Nothing to do for wf: $wfId")
          }
        }
      )

    // De-activation means that we update the state of the workflow to
    // 'forced_termination' and the workers will be shutdown.
    case StopWorkflow(wfId) ⇒
      Either.cond(activeWorkflows.contains(wfId),
      {
        for {
          ns  ← EitherT(stopWorkflow(wfId))
          wfs ← EitherT(deactivateWorkers(wfId)(activeWorkflows))
        } yield {
          logger.info("[Engine][StopWorkflow] {} nodes were updated for workflow id:{} and should be stopped.", ns, wfId)
          activeWorkflows = wfs
        }
      },
        logger.error("[Engine][StopWorkflow] Attempting to stop a workflow id:{} that does not exist!", wfId)
      )

    // TODO: Handle the termination of the child actor
    case Terminated(child) ⇒
      logger.debug("[Engine][Internal] The job {} has terminated.", child)
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
  def startJobs(wfId: WorkflowId)(active: Map[WorkflowId, Set[ActorRef]]) : Reader[Vector[Job], Either[Map[WorkflowId, Set[ActorRef]],Map[WorkflowId, Set[ActorRef]]]] = Reader{ (jobs: Vector[Job]) ⇒
    val startedNodes : Either[Throwable, Vector[Option[Boolean]]] = jobs.map(job ⇒ updateWorkflow(wfId)(job.id)(JobStates.start)).sequence
    startedNodes.bimap(
      (err: Throwable ) ⇒ active,
      (ys: Vector[Option[Boolean]]) ⇒ {
        val workers = createWorkers(Set(jobs:_*))
        activateWorkers(wfId)(workers)
        collection.immutable.Map(
          addToActive(wfId)(workers.map(_._1)).runS(collection.mutable.Map(active.toList:_*)).value.toList :_*
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
  def dropWorkflowFromActive(active: Map[WorkflowId, Set[ActorRef]], failed: Map[WorkflowId, Set[ActorRef]]) = Reader{ (wfId: WorkflowId) ⇒
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
  def logFailure : Reader[Throwable, Unit] = Reader{(t: Throwable) ⇒
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
    actors.map(actor ⇒ actor._1 ! StartRun(wfId, actor._2, self))
  }

  /**
    * Attempts to discover the FSM(s) associated with the given workflow id and
    * issue the [[StopRun]] command
    * @param wfId - workflow id
    * @param xs - state data
    * @return a Left which indicates a error condition or a Right which indicates success.
    */
  def deactivateWorkers(wfId: WorkflowId) : Reader[WFA, Either[String, WFA]] = Reader { (workflows: WFA) ⇒
    Either.cond(
      workflows.contains(wfId),
      {
        workflows(wfId).map(actor ⇒ actor ! StopRun)
        removeFromActive(wfId).runS(collection.mutable.Map(workflows.toSeq: _*)).value
      },
      s"[DeactivateWorkers] Did not discover workflow $wfId in the internal state."
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
    Set( Yoneda(jobs.toList).map( job ⇒ (lift[Id].run(createWorker(context)) >>= (worker ⇒ (watchWorker(worker, job)(context))))).run : _*)
  }

  /**
    * Creates the actor using the context object
    * @param ctx this actor's context
    * @return an actor of type [[JobFSM]]
    */
  def createWorker = Reader{(ctx: ActorContext) ⇒ ctx.actorOf(Props(classOf[JobFSM]))}

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

object Engine extends App {

  /**
    * With [[Props]], we can create all kinds of different [[Engine]] actors
    * with various properties
    */
  def props : Props = Props(classOf[Engine])

  // Load all the properties of the engine e.g. size of thread pool, timeouts
  // for the various parts of the engine while processing.

  val waitTimeForCleanup = 4000
  val waitTimeForAsyncProcessing = 30000

  val actorSystem = ActorSystem("EngineSystem")
  val engine = actorSystem.actorOf(Props(classOf[Engine]), "Engine")

  // load a job graph
  val jobGraph = CLRSBumsteadGraph.workflow // some graph

  // start a job graph running
  engine ! StartWorkflow(jobGraph)

  Thread.sleep(waitTimeForAsyncProcessing)

  // stops the workflow aka "forced termination" of the jobgraph
  engine ! StopWorkflow(jobGraph.id)

  Thread.sleep(waitTimeForCleanup)

  // Stop the engine
  actorSystem.terminate()
}

