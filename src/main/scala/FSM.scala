package hicoden.jobgraph.fsm

import hicoden.jobgraph.Job
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
case class StartRun(job : Job, engine: ActorRef)
case object StopRun
case object SS

// States
sealed trait State
case object Idle extends State
case object Active extends State
case class Terminated(forced: Boolean = false) extends State

// Data
sealed trait Data
case object Uninitialized extends Data
case class StateData(x: Int) extends Data

class JobFSM extends LoggingFSM[State, Data] {

  startWith(Idle, Uninitialized)

  when (Idle, stateTimeout = 1 second) {
    case Event(SS, sss) =>
      log.info("Got an SS msg")
      goto(Active) using StateData(444444)
    case Event(StateTimeout, data) ⇒
      log.info("Timed out from idle, going to active")
      goto(Active) using StateData(4444)

    case Event(StartRun(job, engine), Uninitialized) ⇒
      log.info("Received the job: {}", job)
      stay
  }

  onTransition {
    case Idle -> Active ⇒
      log.info("entering 'Active' from 'Idle'")
    case Active -> Idle ⇒
      log.info("entering 'Idle' from 'Active'")
  }

  when(Active, stateTimeout = 1 second) {
    case Event(SS, sss) =>
      log.info("Got an SS msg")
      stay
    case Event(evt, StateData(x)) ⇒
      log.info("Got this datum: {}, from the event: {}", x, evt)
      goto(Idle)
    case Event(StateTimeout, data) ⇒
      log.info("Timed out after 1 seconds, going back to idle")
      goto(Idle)
  }

  initialize()
}

object FSM extends App {

  val aS = ActorSystem("fsm")
  val a = aS.actorOf(Props(classOf[JobFSM]))
  a ! SS
  Thread.sleep(5000)
  aS.terminate
}
