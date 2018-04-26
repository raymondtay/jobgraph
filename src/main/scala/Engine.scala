package hicoden.jobgraph.engine

import hicoden.jobgraph._
import akka.actor._
import scala.concurrent.duration._
import hicoden.jobgraph.Graph // a Digraph scatter-gatther
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

  import WorkflowOps._

  def receive : PartialFunction[Any, Unit] = {

    case StartWorkflow(jobGraph) ⇒
      logger.debug("[Engine] Received a job graph")
      startWf(jobGraph.id)
      logger.info("[Engine] Started a job graph")

    case UpdateWorkflow(wfId, jobId, signal) ⇒
      updateWorkflow(wfId)(jobId)(signal)
      // update the workflow by some ID, the signal should contain the ID of
      // the step together with the status object
      //
      // at this time, the engine will decide whether to push for the execution
      // of the next step(s) (depending on whether it needs to wait) or
      // terminate the workflow
    case StopWorkflow(wfId) ⇒
      stopWorkflow(wfId)
      // stop the worker by terminating it
  }

}

object Engine extends App {

  // Load all the properties of the engine e.g. size of thread pool, timeouts
  // for the various parts of the engine while processing.

  val actorSystem = ActorSystem("EngineSystem")
  val engine = actorSystem.actorOf(Props(classOf[Engine]), "Engine")

  // load a job graph
  val jobGraph = Graph.workflow // some graph

  // start a job graph running
  engine ! StartWorkflow(jobGraph)

  // Stop the engine
  actorSystem.terminate()
}

