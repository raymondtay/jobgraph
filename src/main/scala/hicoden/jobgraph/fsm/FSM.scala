package hicoden.jobgraph.fsm

import hicoden.jobgraph.{Job, JobId, JobStates, WorkflowId}
import hicoden.jobgraph.engine.UpdateWorkflow
import hicoden.jobgraph.configuration.engine.model.MesosConfig
import hicoden.jobgraph.fsm.runners._
import hicoden.jobgraph.fsm.runners.runtime._
import akka.actor._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import cats._, data._, implicits._

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
// terminate the existing work gracefully; what is meant by gracefully here is
// that we shall send a signal to Apache Beam et al (Apache Beam shall take
// care of terminating the job based on its semantics).

// Events
case class StartRun(wfId: WorkflowId, job : Job, engine: ActorRef, mesosConfig: Option[MesosConfig])
case object StopRun
case object Go
case class MonitorRun(wfId: WorkflowId, jobId: JobId, engine: ActorRef, googleDataflowId: String)

// States
sealed trait State
case object Idle extends State
case object Active extends State
case class Terminated(forced: Boolean = false) extends State

// Data
sealed trait Data
case object Uninitialized extends Data
case class Processing(wfId: WorkflowId, job: Job, engineRef: ActorRef, mesosConfig : Option[MesosConfig]) extends Data

class JobFSM extends LoggingFSM[State, Data] {

  implicit val googleDataflowDispatcher : ExecutionContext = context.system.dispatchers.lookup("dataflow-dispatcher")

  startWith(Idle, Uninitialized)

  // TODO:
  // (a) Lift timeout to configuration
  // (b) List out the Engine-triggered events vs FSM internal events
  when (Idle, stateTimeout = 2 second) {

     case Event(StopRun, _) ⇒
      log.info("Stopping run")
      stop(FSM.Shutdown)

    case Event(StateTimeout, data) ⇒
      log.error("Did not receive any signal from Engine, stopping.")
      stop(FSM.Failure("Did not receive signal from Engine within the time limit."))

    case Event(StartRun(wfId, job, engineActor, mesosCfg), Uninitialized) ⇒
      log.info("Received the job: {} for workflow: {}", job.id, wfId)
      goto(Active) using Processing(wfId, job, engineActor, mesosCfg)
  }

  onTransition {
    case Idle -> Active ⇒
      log.info("entering 'Active' from 'Idle'")
      setTimer("Start Job", Go, timeout = 1 second)
    case Active -> Active ⇒
      log.info("You called Sire?")
  }

  when(Active) {

    case Event(StopRun, _) ⇒
      log.info("Stopping run")
      stop(FSM.Shutdown)

    case Event(Go, Processing(wfId, job, engineRef, mesosCfg)) ⇒
      log.info(s"""
        About to start ${wfId}
        """)
      engineRef ! UpdateWorkflow(wfId, job.id, JobStates.active)

      mesosCfg.fold[scala.concurrent.Future[_]]{ // not doing anything with the async result so its going to Future[_]
        val ctx = ExecContext(job.config)
        val runner = new DataflowRunner
        runner.run(ctx)(JobContextManifest.manifest(wfId, job.id))
      }{mCfg ⇒ 
        if (mCfg.enabled) {
          val ctx = MesosExecContext(job.config, mCfg)
          val runner = new MesosDataflowRunner
          runner.run(ctx)(JobContextManifest.manifestMesos(wfId, job.id, mCfg))
        } else {
          val ctx = ExecContext(job.config)
          val runner = new DataflowRunner
          runner.run(ctx)(JobContextManifest.manifest(wfId, job.id))
        }
      }
      stay

    case Event(MonitorRun(wfId, jobId, engineRef, googleDataflowId), _) ⇒
      val ctx = MonitorContext(getClass.getClassLoader.getResource("gcloud_monitor_job.sh").getPath.toString :: Nil,
                               googleDataflowId,
                               io.circe.Json.Null)
      val runner = new DataflowMonitorRunner
      GoogleDataflowFunctions.interpretJobResult(runner.run(ctx)(jsonParser.parse)).
        fold(forcedStop(wfId, jobId, engineRef)(googleDataflowId))(decideToStopOrContinue(wfId, jobId, engineRef, googleDataflowId)(_))


    case Event(StateTimeout, Processing(wfId, job, engineRef, mesosCfg)) ⇒
      log.info("Finished processing, shutting down.")
      engineRef ! UpdateWorkflow(wfId, job.id, JobStates.finished)
      stop(FSM.Shutdown)

  }

  /**
    * Invoked iff when we cannot parse the result in JSON format
    * @param wfId
    * @param jobId
    * @param engineRef
    * @param googleDataflowId
    * @return Transition to [[FSM.Failure]] state
    */
  def forcedStop(wfId: WorkflowId, jobId: JobId, engineRef: ActorRef) =
    Reader{ (googleDataflowId: String) ⇒
      engineRef ! UpdateWorkflow(wfId, jobId, JobStates.forced_termination)
      stop(FSM.Failure(s"[FSM] could not parse the response from Google as Json for jobId: $googleDataflowId, forced abort."))
    }

  /**
    * Interprets the result (after parsing the JSON structure returned by
    * Google Dataflow). The job's state is updated for the following states:
    * (a) JOB_STATE_DONE => normal completion
    * (b) JOB_STATE_CANCELLED => forced termination so job will reflect the same
    * (c) everything else will mean it will continue to be monitored
    * @param wfId
    * @param jobId
    * @param engineRef
    * @param googleDataflowId
    * @return Transition to either (a) same state i.e. Active, (b) stops either
    * normally or Shutsdown
    */
  def decideToStopOrContinue(wfId: WorkflowId, jobId: JobId, engineRef: ActorRef, googleDataflowId: String) = Reader{ (result: (String,String,GoogleDataflowJobStatuses.GoogleDataflowJobStatus, String)) ⇒
    if (result._3 equals GoogleDataflowJobStatuses.JOB_STATE_DONE) {
      engineRef ! UpdateWorkflow(wfId, jobId, JobStates.finished)
      stop(FSM.Normal)
    }
    else if (result._3 equals GoogleDataflowJobStatuses.JOB_STATE_CANCELLED) {
      engineRef ! UpdateWorkflow(wfId, jobId, JobStates.forced_termination)
      stop(FSM.Shutdown)
    } else {
      setTimer("Monitor Job", MonitorRun(wfId, jobId, engineRef, googleDataflowId), timeout = 10 second)
      stay
    }
  }

  initialize()
}

