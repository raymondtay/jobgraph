package hicoden.jobgraph.fsm

import hicoden.jobgraph.cc.{LRU, RealHttpService}
import hicoden.jobgraph.{Job, JobId, JobStates, WorkflowId}
import hicoden.jobgraph.engine.{UpdateWorkflow, DataflowFailure}
import hicoden.jobgraph.configuration.engine.model.{MesosConfig, JobgraphConfig}
import hicoden.jobgraph.fsm.runners._
import hicoden.jobgraph.fsm.runners.runtime._
import akka.actor._
import akka.pattern._
import akka.stream.ActorMaterializer
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
case class StartRun(wfId: WorkflowId, job : Job, engine: ActorRef, mesosConfig: Option[MesosConfig], jobgraphConfig: Option[JobgraphConfig])
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
case class Processing(wfId: WorkflowId,
                      job: Job,
                      engineRef: ActorRef,
                      mesosConfig : Option[MesosConfig],
                      jobgraphConfig: Option[JobgraphConfig]) extends Data

/**
  * JobFSM is the caretaker of a job, infact any Job really. What it is suppose
  * to do is to kick start the job, attempting to dispatch to running Apache
  * Mesos cluster(s) and monitors the job's status (iff its a Dataflow job)
  * otherwise it waits for a status update from the job.
  * [[timeToWait]] - indicates how long to wait before exiting with a general
  * failure, which will trigger a restart
  */
class JobFSM(timeToWait: Int) extends LoggingFSM[State, Data] with JobContextManifest with JobFSMFunctions {

  implicit val actorSystem = context.system
  implicit val actorMaterializer = ActorMaterializer()
  implicit val googleDataflowDispatcher : ExecutionContext = context.system.dispatchers.lookup("dataflow-dispatcher")
  override val supervisorStrategy = 
    OneForOneStrategy() {
      case _: DataflowFailure ⇒ SupervisorStrategy.Escalate
    }

  startWith(Idle, Uninitialized)

  when (Idle, stateTimeout = 2 second) {

     case Event(StopRun, _) ⇒
      log.info("Stopping run")
      stop(FSM.Shutdown)

    case Event(StateTimeout, data) ⇒
      log.error("Did not receive any signal from Engine, stopping.")
      stop(FSM.Failure("Did not receive signal from Engine within the time limit."))

    case Event(StartRun(wfId, job, engineActor, mesosCfg, jgCfg), Uninitialized) ⇒
      log.info("Received the job: {} for workflow: {}", job.id, wfId)
      goto(Active) using Processing(wfId, job, engineActor, mesosCfg, jgCfg)
  }

  onTransition {
    case Idle -> Active ⇒
      log.info("entering 'Active' from 'Idle'")
      setTimer("Start Job", Go, timeout = 1 second)
    case Active -> Active ⇒
      log.info("You called Sire?")
  }

  when(Active, stateTimeout = (timeToWait * secondsIn1Minute).seconds) {

    case Event(StopRun, _) ⇒
      log.info("Stopping run")
      stop(FSM.Shutdown)

    case Event(Go, Processing(wfId, job, engineRef, mesosCfg, jgCfg)) ⇒
      log.info(s"""
        About to start ${wfId}
        """)
      engineRef ! UpdateWorkflow(wfId, job.id, JobStates.active)

      (mesosCfg, jgCfg).mapN[scala.concurrent.Future[_]]{ // not doing anything with the async result so its going to Future[_]
        (mCfg, jgCfg) ⇒
        if (mCfg.enabled) {
          val ctx = MesosExecContext(job.config, mCfg)
          val runner = new MesosDataflowRunner
          implicit val mesosRequestTimeout : Duration = mCfg.timeout.seconds
          runner.run(ctx)(manifestMesos(wfId, job.id, mCfg, jgCfg, new RealHttpService, select)(LRU))
        } else {
          val ctx = ExecContext(job.config)
          val runner = new DataflowRunner
          runner.run(ctx)(manifest(wfId, job.id, jgCfg))
        }
      }
      stay

    case Event(MonitorRun(wfId, jobId, engineRef, googleDataflowId), _) ⇒
      val ctx = MonitorContext(getClass.getClassLoader.getResource("gcloud_monitor_job.sh").getPath.toString :: Nil,
                               googleDataflowId,
                               io.circe.Json.Null)
      val runner = new DataflowMonitorRunner
      GoogleDataflowFunctions.interpretJobResult(runner.run(ctx)(jsonParser.parse)).
        fold(stop(forcedStop(wfId, jobId, engineRef)(googleDataflowId)))(
          decideToStopOrContinue(wfId, jobId, engineRef, googleDataflowId)(_).fold{
           setTimer("Monitor Job", MonitorRun(wfId, jobId, engineRef, googleDataflowId), timeout = 10 second)
           stay
          }(sideEffect ⇒ stop(sideEffect()))
        )

    case Event(StateTimeout, Processing(wfId, job, engineRef, mesosCfg, jgCfg)) ⇒
      log.info(s"Timeout after ${timeToWait * secondsIn1Minute} seconds; marking job:[${job.id}] as failed.")
      log.debug(s"Timeout after ${timeToWait * secondsIn1Minute} seconds; marking workflow:[$wfId], job:[${job.id}] as failed.")
      engineRef ! UpdateWorkflow(wfId, job.id, JobStates.failed)
      stop(FSM.Shutdown)

  }

  initialize()
}


private[fsm]
trait JobFSMFunctions {

  val secondsIn1Minute = 60

  /**
    * Interprets the result (after parsing the JSON structure returned by
    * Google Dataflow). The job's state is updated for the following states:
    * (a) JOB_STATE_DONE => normal completion
    * (b) JOB_STATE_CANCELLED => forced termination so job will reflect the same
    * (c) JOB_STATE_FAILED => this job will be restarted
    * (d) everything else will mean it will continue to be monitored
    * @param wfId
    * @param jobId
    * @param engineRef
    * @param googleDataflowId
    * @return A side-effect which effectively translate (during runtime) to transition
    *        to either (a) same state i.e. Active, (b) stops either normally or Shutsdown
    */
  def decideToStopOrContinue(wfId: WorkflowId, jobId: JobId, engineRef: ActorRef, googleDataflowId: String) : Reader[(String,String,GoogleDataflowJobStatuses.GoogleDataflowJobStatus,String), Option[() ⇒ FSM.Reason]] =
    Reader{ (result: (String,String,GoogleDataflowJobStatuses.GoogleDataflowJobStatus, String)) ⇒
      if (result._3 equals GoogleDataflowJobStatuses.JOB_STATE_FAILED) {
        (() ⇒ {
          throw new DataflowFailure
          FSM.Failure(s"Dataflow job: $jobId of workflow: $wfId has failed.")
        }).some
      }
      else if (result._3 equals GoogleDataflowJobStatuses.JOB_STATE_DONE) {
        (() ⇒ {
          engineRef ! UpdateWorkflow(wfId, jobId, JobStates.finished)
          FSM.Normal
        }).some
      }
      else if (result._3 equals GoogleDataflowJobStatuses.JOB_STATE_CANCELLED) {
        (() ⇒ {
          engineRef ! UpdateWorkflow(wfId, jobId, JobStates.forced_termination)
          FSM.Shutdown
        }).some
      } else {
        None
      }
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
      FSM.Failure(s"[FSM] could not parse the response from Google as Json for jobId: $googleDataflowId, forced abort.")
    }

}

