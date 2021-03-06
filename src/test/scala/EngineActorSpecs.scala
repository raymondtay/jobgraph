package hicoden.jobgraph.engine


import hicoden.jobgraph.{Job, WorkflowId, Workflow}

import scala.concurrent.duration._
import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.pattern._
import akka.actor._
import akka.testkit._
import org.scalatest.{ BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike }

//
// Note: Tracing the log messages by tailing the behind of actors can be a
// little tricky if you are not careful. If tests borked after some upgrade, i
// would suggest to look at those actor names as a possible source of errors.
//
class EngineActorSpecs() extends TestKit(ActorSystem("EngineActorSpecs")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Engine" must {

    "not proceed to execute if the workflow id is not found; when no jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], Some(true), Nil, Nil), "Engine-1")
      val nonExistentId = 100

      engine ! StartWorkflow(jobOverrides = None, nonExistentId)
      expectMsg("No such id")
    }

    "not proceed to execute if the workflow id is not found; when jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], Some(true), "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-2")
      val existentId = 1
      engine ! StartWorkflow(jobOverrides = None, existentId)
      expectMsg("No such id")
    }

    "proceed to execute if the workflow id is found after jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], Some(true), "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-3")
      val existentId = 18
      within(3 second) {
        engine ! StartWorkflow(jobOverrides = None, existentId)
        Thread.sleep(2000)
        expectMsgType[String] // in truth, the runtime generated workflow-id (in UUID format) is generated and returned.
      }
    }

    "proceed to execute concurrently if the workflow id is found after jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], Some(true), "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-4")
      val existentId = 19
      within(10 second) {
        engine ! StartWorkflow(jobOverrides = None, existentId)
        Thread.sleep(4000)
        expectMsgType[String] // in truth, the runtime generated workflow-id (in UUID format) is generated and returned.
      }

    }

    "not proceed to execute if the workflow id is found but there's a loop; when jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], Some(true), "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-5")
      val existentId = 20
      engine ! StartWorkflow(jobOverrides = None, existentId)
      Thread.sleep(2000)
      expectMsgType[String]
    }

    "updating the workflow that ∉ of the memory of jobgraph is bound to fail with an entry in the logs." in {
      import hicoden.jobgraph.JobStates
      val engine = system.actorOf(Props(classOf[Engine], Some(true), "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-6")
      val invalidWfId = java.util.UUID.randomUUID // these ids do not exist during the test since its generated runtime and we didn't capture a`prior.
      val invalidJobId = java.util.UUID.randomUUID
      engine ! UpdateWorkflow(invalidWfId, invalidJobId, JobStates.forced_termination)
      expectNoMessage()
    }

    "updating the workflow that ∈ of the memory of jobgraph is bound to succeed, and sets off the next job in line." in {
      import hicoden.jobgraph.JobStates
      val engine = system.actorOf(Props(classOf[Engine], Some(true), "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-7")
      val existentId = 18
      engine ! StartWorkflow(jobOverrides = None, existentId)

      Thread.sleep(3000) // sleep a little so that the external process can bootup.

      var runtimeWorkflowId : WorkflowId = null
      expectMsgPF(1 second){ case msg ⇒ runtimeWorkflowId = java.util.UUID.fromString(msg.asInstanceOf[String]) }
      engine ! StopWorkflow(runtimeWorkflowId)
      expectNoMessage()
    }

  }

}

