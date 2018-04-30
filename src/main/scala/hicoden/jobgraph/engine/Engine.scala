package hicoden.jobgraph.engine

import hicoden.jobgraph._
import akka.actor._
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
class Engine extends Actor with ActorLogging {
  import cats._, data._, implicits._
  import WorkflowOps._

  // TODO:
  // (a) ADTs should be accessible from any node in the cluster
  //
  private[this] var activeWorkflows = collection.mutable.Map.empty[WorkflowId, Set[ActorRef]]
  private[this] var workflowsReceived = collection.mutable.Map.empty[WorkflowId, Workflow]

  def receive : PartialFunction[Any, Unit] = {

    case StartWorkflow(jobGraph) ⇒
      logger.debug("[Engine] Received a job graph")
      val workers = startWorkflow(jobGraph.id).fold(Set.empty[(ActorRef, Job)])(createWorkers(_))
      if (workers.isEmpty) {
        logger.error("[Engine] System error detected as there are no start nodes, check it!")
      } else {
        activateWorkers(jobGraph.id)(workers)
        activeWorkflows += jobGraph.id → workers.map(_._1)
        logger.info("[Engine] Started a job graph")
      }

    case UpdateWorkflow(wfId, jobId, signal) ⇒
      updateWorkflow(wfId)(jobId)(signal).bimap(
        whenFailure.run,
        (status: Option[Boolean]) ⇒ decipher(wfId)(jobId)
      )

    case StopWorkflow(wfId) ⇒
      if (activeWorkflows.contains(wfId)) {
        stopWorkflow(wfId) // state should be updated to 'forced_termination'
        deactivateWorkers(wfId)(activeWorkflows.toMap).bimap(
          whenFailure.run,
          returnedMap ⇒ activeWorkflows = collection.mutable.Map.empty[WorkflowId, Set[ActorRef]] ++ returnedMap
        )
        sender ! s"Workflow $wfId stopped"
      } else sender ! s"Workflow $wfId does not exist"

    case Terminated(child) ⇒
      logger.info("[Engine] The job {} has terminated.", child)
  }

  /**
    * Deciphers the graph structure for the workflow and decides whether to
    * start other nodes
    */
  def decipher(wfId: WorkflowId) = Reader{ (jobId: JobId) ⇒
    // find all the successor nodes from the given job that has not been
    // started and ready to be started, respecting the constraints.
    true.some
  }

  /**
    * General function invoked when error encountered
    * @param t - [[java.lang.Throwable]] object representing errors
    * @return nothing - with the side effect that logs are pumped
    */
  def whenFailure = Reader{(t: Throwable) ⇒
    logger.error("[whenFailure] Error encountered with details")
    logger.debug(s"[whenFailure] Stack Trace: ${t.getStackTrace.map(println(_))}")
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
  def deactivateWorkers(wfId: WorkflowId) = Reader { (workflows: Map[WorkflowId, Set[ActorRef]]) ⇒
    Either.cond(
      workflows.contains(wfId),
      {
        workflows(wfId).map(actor ⇒ actor ! StopRun)
        workflows - wfId
      },
      throw new Exception(s"[DeactivateWorkers] Did not discover workflow $wfId in the internal state.")
    )
  }

  /**
    * Basically creates the Actor and associates its to the job; take note that
    * the engine watches over the lifecycle of the worker too
    * @param jobs - container of jobs to associate
    * @return set - a set of (k,v) pairs where the actor is associated with the
    * job
    */
  def createWorkers : Reader[Set[Job], Set[(ActorRef, Job)]] = Reader{ (jobs: Set[Job]) ⇒
    jobs.map{job ⇒
      val worker = context.actorOf(Props(classOf[JobFSM]))
      context.watch(worker)
      (worker, job )
    }
  }

}

object Engine extends App {

  // Load all the properties of the engine e.g. size of thread pool, timeouts
  // for the various parts of the engine while processing.

  val actorSystem = ActorSystem("EngineSystem")
  val engine = actorSystem.actorOf(Props(classOf[Engine]), "Engine")

  // load a job graph
  val jobGraph = ConvergeGraph.workflow // some graph

  // start a job graph running
  engine ! StartWorkflow(jobGraph)

  Thread.sleep(8000)

  engine ! StopWorkflow(jobGraph.id)

  Thread.sleep(4000)

  // Stop the engine
  actorSystem.terminate()
}

