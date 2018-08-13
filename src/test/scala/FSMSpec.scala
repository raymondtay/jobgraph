package hicoden.jobgraph.fsm

import hicoden.jobgraph.Job
import hicoden.jobgraph.configuration.step.model.{JobConfig, Restart, Runner}


import akka.actor._
import akka.testkit._
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}
import org.scalatest.{ BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike }
import akka.testkit.EventFilter
import com.typesafe.config.ConfigFactory

object EventLoggingEnabled {

implicit def system(actorSystemName: String) =
  ActorSystem(actorSystemName,
              ConfigFactory.parseString("""
                mesos {
                  enabled  : true
                  runas    : hicoden
                  hostname : localhost
                  hostport : 5050
                }
                dataflow-dispatcher {
                  type = Dispatcher
                  executor = "thread-pool-executor"
                  thread-pool-executor {
                    fixed-pool-size = 4
                  }
                  throughput = 1
                }
                akka.loglevel = DEBUG
                akka.loggers = ["akka.testkit.TestEventListener"]
              """))
}

class EngineDummy extends Actor with ActorLogging {
  def receive = {
    case msg ⇒
  }
}

class JobFSMSpecs() extends TestKit(EventLoggingEnabled.system("JobFSMSpecs")) with
      ImplicitSender with
      WordSpecLike with
      Matchers with
      BeforeAndAfterEach with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "JobFSM always starts at the Idle state" in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM)

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
  }

  "JobFSM when in 'Idle' state, would end in failure after not receiving any signal from Engine after the timeout expires." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM, name = "the-fsm-1")

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
    within(3 seconds) { // Idle state is expecting to timed out after 2 seconds, when 'fsm' does nothing.
      EventFilter.error(source = "akka://JobFSMSpecs/users/the-fsm-1", message = "Did not receive any signal from Engine, stopping.").intercept {
        fsm
      }
    }
  }

  "JobFSM when in 'Idle' state, would stop its run when encountered 'StopRun'." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM, name = "the-fsm-2")

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
    within(3 seconds) { // Idle state is expecting to timed out after 2 seconds but we are not going to hit that since we are firing 'StopRun'
      EventFilter.info(source = "akka://JobFSMSpecs/user/the-fsm-2", message = "Stopping run", occurrences = 1).intercept {
        fsm ! StopRun
      }
    }
  }

  "JobFSM when in 'Idle' state, would transition to 'Active' state when encountered 'StartRun'." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM) // the actual JobFSM actor
    val engine = TestActorRef(new EngineDummy) // the fake Engine actor

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    val wfId = java.util.UUID.randomUUID
    val jobId = Job("job1")

    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
    fsm ! StartRun(wfId, jobId, engine, None, None)
    fsm.stateName == Active
    fsm.isTimerActive("Start Job") == true
  }

  "JobFSM when in 'Active' state, would stop its run when encountered 'StopRun'." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM, name = "the-fsm-3") // the actual JobFSM actor
    val engine = TestActorRef(new EngineDummy) // the fake Engine actor

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    val wfId = java.util.UUID.randomUUID
    val jobId = Job("job1")

    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
    fsm ! StartRun(wfId, jobId, engine, None, None)
    fsm.stateName == Active
    fsm.isTimerActive("Start Job") == true
 
    within(3 seconds) { // Idle state is expecting to timed out after 2 seconds but we are not going to hit that since we are firing 'StopRun'
      EventFilter.info(source = "akka://JobFSMSpecs/user/the-fsm-3", message = "Stopping run", occurrences = 1).intercept {
        fsm ! StopRun
      }
    }
  }

  "JobFSM when in 'Active' state, would proceed its run after waiting for a short period (e.g. 1 second)." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM, name = "the-fsm-4") // the actual JobFSM actor
    val engine = TestActorRef(new EngineDummy) // the fake Engine actor

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    val wfId = java.util.UUID.randomUUID
    val job  = Job("job1")

    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
 
    within(2 seconds) {
      EventFilter.info(source = "akka://JobFSMSpecs/user/the-fsm-4", pattern = "About to start", occurrences = 1).intercept {
        fsm ! StartRun(wfId, job, engine, None, None)
        fsm.stateName == Active
        fsm.isTimerActive("Start Job") == true
      }
    }
  }

  "JobFSM when in 'Active' state, would proceed its run after waiting for a short period (e.g. 1 second); begin its automatic monitoring run when activated and this run terminates." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM, name = "the-fsm-5") // the actual JobFSM actor
    val engine = TestActorRef(new EngineDummy) // the fake Engine actor
    val probe = TestProbe("JobFSMSpecs")
    probe.watch(fsm) // watch the [[JobFSM]] actor



    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    val wfId = java.util.UUID.randomUUID
    val job  = Job("job1")
    val googleDataflowId = "fake-google-dataflow-id"

    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
 
    within(8 seconds) {
      EventFilter.info(source = "akka://JobFSMSpecs/user/the-fsm-5", pattern = "About to start", occurrences = 1).intercept {
        fsm ! StartRun(wfId, job, engine, None, None)
        fsm.stateName == Active
        fsm.isTimerActive("Start Job") == true
        fsm ! Go
        fsm ! MonitorRun(wfId, job.id, engine, googleDataflowId)
      }
    }

    probe.expectTerminated(fsm)
  }

  "JobFSM when in 'Active' state, would proceed its run after waiting for a short period (e.g. 1 second); begin its automatic monitoring run when activated." in {
    import scala.concurrent.duration._
    val fsm = TestFSMRef(new JobFSM, "the-fsm-6") // the actual JobFSM actor
    val engine = TestActorRef(new EngineDummy, "engine-6") // the fake Engine actor
    val probe = TestProbe("JobFSMSpecs")
    probe.watch(fsm) // watch the [[JobFSM]] actor so that we can catch its end-of-life.


    val jobConfig =
      JobConfig(id = 42, name = "job-config-1", description = "",
        workdir = "", sessionid = "", timeout = 4,
        restart = Restart(3),
        runner = Runner(runner = "Dataflow:java", module = getClass.getClassLoader.getResource("fake_start_dataflow_job.sh").getPath.toString,
        cliargs = Nil))

    val mustBeTypedProperly: TestActorRef[JobFSM] = fsm
    val wfId = java.util.UUID.randomUUID
    val job  = Job("job1", jobConfig)
    val googleDataflowId = "fake-google-dataflow-id"

    fsm.stateName == Idle
    fsm.isTimerActive("Start Job") == false
 
    within(8 seconds) {
      EventFilter.info(source = "akka://JobFSMSpecs/user/the-fsm-6", pattern = "About to start", occurrences = 1).intercept {
        fsm ! StartRun(wfId, job, engine, None, None)
        fsm.stateName == Active
        fsm.stateData == Processing(wfId, job, engine, None, None)
        fsm.isTimerActive("Start Job") == true
        Thread.sleep(3000)
        fsm ! MonitorRun(wfId, job.id, engine, googleDataflowId)
      }
    }

    probe.expectTerminated(fsm)
  }

}
