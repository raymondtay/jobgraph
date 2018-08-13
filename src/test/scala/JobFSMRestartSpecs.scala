package hicoden.jobgraph.fsm

import scala.concurrent._
import scala.concurrent.duration._
import akka.actor._
import akka.pattern._
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.{BeforeAndAfterEach, WordSpecLike, Matchers, BeforeAndAfterAll }
import akka.testkit.{ TestActors, TestKit, ImplicitSender, EventFilter }
import hicoden.jobgraph.{Job, JobId, WorkflowId}
import hicoden.jobgraph.configuration.step.model._
import hicoden.jobgraph.engine._
import hicoden.jobgraph.fsm.runners._
import hicoden.jobgraph.fsm.runners.runtime._

/**
  * Each of the [[JobFSM]] actors start up with the capability to restart with
  * a back-off option (following a exponential backoff algorithm by Akka) and
  * we have added some noise of about 20% to the calculation so that in the
  * case of causing massive CPU spikes in the running system; overall this
  * strategy should ease CPU spikes and resource contention during a restart
  *
  * @author Raymond Tay
  * @date 17 July 2018
  */

case class Trigger(wfId: WorkflowId,
                   jobConfig: JobConfig,
                   job: Job,
                   engine: ActorRef,
                   dataflowId: String,
                   monitoredResult: (String,String, GoogleDataflowJobStatuses.GoogleDataflowJobStatus, String))

case class TriggerRestart(wfId: WorkflowId,
                   jobConfig: JobConfig,
                   job: Job,
                   engine: ActorRef,
                   dataflowId: String,
                   monitoredResult: (String,String, GoogleDataflowJobStatuses.GoogleDataflowJobStatus, String))

case class TriggerStoppage(wfId: WorkflowId,
                   jobConfig: JobConfig,
                   job: Job,
                   engine: ActorRef,
                   dataflowId: String,
                   monitoredResult: (String,String, GoogleDataflowJobStatuses.GoogleDataflowJobStatus, String))

class SimulatedRestart extends Actor with JobFSMFunctions {
  var currentWorkflowId: WorkflowId = null
  var currentJob: Job = null

  def receive = {
    case t@Trigger(wfId, jobConfig, job, engineWorker, "fake google dataflow id", _) ⇒
      currentWorkflowId = wfId
      currentJob = job
      decideToStopOrContinue(wfId, job.id, engineWorker, t.dataflowId)(t.monitoredResult) match {
        case Some(f) ⇒ f()
        case _ ⇒ /* only testing the Some(side-effect) */
      }
  }
}

class SimulatedStoppage extends Actor with JobFSMFunctions {
  var currentWorkflowId: WorkflowId = null
  var currentJob: Job = null

  def receive = {
    case t@Trigger(wfId, jobConfig, job, engineWorker, "fake google dataflow id", _) ⇒
      currentWorkflowId = wfId
      currentJob = job
      forcedStop(wfId, job.id, sender())(t.dataflowId) match {
        case msg : FSM.Failure ⇒ sender() ! "invalid google dataflow id"
        case _ ⇒
      }
  }
}


class SimulatedMaster extends Actor {

  var originalSender : ActorRef = null

  def receive = {

    case t@TriggerStoppage(wfId, jobConfig, job, engineWorker, "fake google dataflow id", _) ⇒
      originalSender = sender()
      val jobWorker =
        context.actorOf(backoffOnFailureStrategy(Props(classOf[SimulatedStoppage]), s"SimulatedStoppage@${java.util.UUID.randomUUID}", jobConfig))

      jobWorker ! Trigger(wfId, jobConfig, job, engineWorker, t.dataflowId, t.monitoredResult)

    case t@TriggerRestart(wfId, jobConfig, job, engineWorker, "fake google dataflow id", _) ⇒
      originalSender = sender()
      val jobWorker =
        context.actorOf(backoffOnFailureStrategy(Props(classOf[SimulatedRestart]), s"SimulatedRestart@${java.util.UUID.randomUUID}", jobConfig))

      jobWorker ! Trigger(wfId, jobConfig, job, engineWorker, t.dataflowId, t.monitoredResult)
      Thread.sleep(3000) // intention as there's a backoff algorithm in place
      jobWorker ! Trigger(wfId, jobConfig, job, engineWorker, t.dataflowId, t.monitoredResult) /* restart once */
      Thread.sleep(3000) // intention as there's a backoff algorithm in place
      jobWorker ! Trigger(wfId, jobConfig, job, engineWorker, t.dataflowId, t.monitoredResult) /* restart twice - this should fail */

    case _ : UpdateWorkflow ⇒ /* not looking for this */

    case x ⇒ originalSender ! x

  }

}

/**
  * The strategy here is to test the behavior of [[JobFSMFunctions]] in a Actor
  * Setting.
  */
class JobFSMRestartSpecs(_system: ActorSystem) extends TestKit(_system) with
    ImplicitSender with
    WordSpecLike with
    Matchers with
    BeforeAndAfterAll with
    BeforeAndAfterEach {

  var master : ActorRef = null

  def this() = this(
    ActorSystem(
    "RestartStrategySpec",
      ConfigFactory.parseString("""
        dataflow-dispatcher {
          type = Dispatcher
          executor = "thread-pool-executor"
          thread-pool-executor {
            fixed-pool-size = 16
          }
          throughput = 1
        }
        akka {
          loggers = ["akka.testkit.TestEventListener"]
          loglevel = "WARNING"
        }
        """)))

  override def beforeAll {}

  override def beforeEach {
    master = system.actorOf(Props[SimulatedMaster])
  }

  override def afterEach {
    master ! Kill
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Restart supervisor (associated with each Job) " must {

    "restart when the JobFSM detects a Failure from Google Dataflow i.e. some runtime errors in Google" in {
      val jobConfig =
        JobConfig(id=0,name="fake job",
                  description="fake description",
                  workdir="",sessionid="",timeout=4,
                  Restart(1), // maximum allowed restarts
                  runner=null) // we are not running this job (since runner is null) coz we just want to test the restart behavior.
      val job = Job(name = "Test Job", config = jobConfig)
      val wfId = java.util.UUID.randomUUID
      val engineRef = master

      val monitoredResult = ("","",GoogleDataflowJobStatuses.JOB_STATE_FAILED, "")
      implicit val timeout : akka.util.Timeout = 4.seconds
      master ! TriggerRestart(wfId, jobConfig, job, engineRef, "fake google dataflow id", monitoredResult)
      Thread.sleep(5000)
      expectNoMessage()
    }

    "stop when the JobFSM detects a Cancellation from Google Dataflow i.e. manual cancellation" in {
      val jobConfig =
        JobConfig(id=0,name="fake job",
                  description="fake description",
                  workdir="",sessionid="",timeout=4,
                  Restart(1), // maximum allowed restarts
                  runner=null) // we are not running this job (since runner is null) coz we just want to test the restart behavior.
      val job = Job(name = "Test Job", config = jobConfig)
      val wfId = java.util.UUID.randomUUID
      val engineRef = master

      val monitoredResult = ("","",GoogleDataflowJobStatuses.JOB_STATE_CANCELLED, "")
      implicit val timeout : akka.util.Timeout = 4.seconds
      master ! TriggerRestart(wfId, jobConfig, job, engineRef, "fake google dataflow id", monitoredResult)
      expectNoMessage()
    }

    "stop when the JobFSM detects a normal termination from Google Dataflow" in {
      val jobConfig =
        JobConfig(id=0,name="fake job",
                  description="fake description",
                  workdir="",sessionid="",timeout=4,
                  Restart(1), // maximum allowed restarts
                  runner=null) // we are not running this job (since runner is null) coz we just want to test the restart behavior.
      val job = Job(name = "Test Job", config = jobConfig)
      val wfId = java.util.UUID.randomUUID
      val engineRef = master

      val monitoredResult = ("","",GoogleDataflowJobStatuses.JOB_STATE_FAILED, "")
      implicit val timeout : akka.util.Timeout = 4.seconds
      val message = Await.result((master ? TriggerStoppage(wfId, jobConfig, job, engineRef, "fake google dataflow id", monitoredResult)).mapTo[String], timeout.duration)
      assert(message == "invalid google dataflow id")
    }
  }

}

