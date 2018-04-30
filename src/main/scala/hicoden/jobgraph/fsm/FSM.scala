package hicoden.jobgraph.fsm

import hicoden.jobgraph.{Job, JobId, JobStates, WorkflowId}
import hicoden.jobgraph.engine.UpdateWorkflow
import akka.actor._
import scala.concurrent.duration._

// scala language imports
import scala.language.postfixOps


// A FSM actor will be started for every step in the workflow in a lazy fashion
// as the creation of the actor in the workflow means that its executable.
//
// for the digraph [a ->b, a -> c] then a FSM actor will be started for 'a'
// only but no actors will b created for 'b' and 'c'.
//
// for the diagraph [a -> b, a -> c, c -> d, b -> d] it follows from the
// previous that a FSM actor will be created for 'a' and when it finishes
// normally, then FSM actors for 'b' and 'c' will be created and they will
// proceeed asynchronously. At this point, 'd' is never run not existed. When
// 'b' and 'c' completes (without errors), then the FSM actor for 'd' is ever
// created.
//
// The execution model is governed by the Engine.

// Each FSM actor would only ever need to send messages to the engine actor
// which is why they carry the reference to it.
//
// Each step/job would ever only be in the 3 states i.e. idle -> active ->
// terminated. Each step would begin its life in the 'idle' state and upon
// receiving a "start" signal, it would transition to the 'active' state and at
// that point it would connect with the Apache Beam installation and stays
// there till it receives a signal that would send it to termination.
//
// Be aware that a forced-termination can be propagated to the job which would
// terminate the existing work.

// Events
case class StartRun(wfId: WorkflowId, job : Job, engine: ActorRef)
case object StopRun

// States
sealed trait State
case object Idle extends State
case object Active extends State
case class Terminated(forced: Boolean = false) extends State

// Data
sealed trait Data
case object Uninitialized extends Data
case class Processing(wfId: WorkflowId, job: Job, engineRef: ActorRef) extends Data

class JobFSM extends LoggingFSM[State, Data] {

  startWith(Idle, Uninitialized)

  // TODO:
  // (a) Lift timeout to configuration
  // (b) List out the Engine-triggered events vs FSM internal events
  when (Idle, stateTimeout = 2 second) {

     case Event(StopRun, _) ⇒
      log.info("Stopping run")
      stop(FSM.Shutdown)

    case Event(StateTimeout, data) ⇒
      log.info("Timed out from idle, going to active")
      goto(Active) using Uninitialized

    case Event(StartRun(wfId, job, engineActor), Uninitialized) ⇒
      log.info("Received the job: {} for workflow: {}", job.id, wfId)
      goto(Active) using Processing(wfId, job, engineActor)
  }

  onTransition {
    case Idle -> Active ⇒
      log.info("entering 'Active' from 'Idle'")
    case Active -> Idle ⇒
      log.info("entering 'Idle' from 'Active'")
  }

  // TODO:
  // (a) Lift timeout to configuration
  // (b) List out the Engine-triggered events vs FSM internal events
  when(Active, stateTimeout = 3 second) {

    case Event(StopRun, _) ⇒
      log.info("Stopping run")
      stop(FSM.Shutdown)

    case Event(StateTimeout, Processing(wfId, job, engineRef)) ⇒
      log.info("Timed out after 3 seconds, going back to Idle")
      engineRef ! UpdateWorkflow(wfId, job.id, JobStates.forced_termination)
      goto(Idle) using Processing(wfId, job, engineRef)
  }

  initialize()
}

