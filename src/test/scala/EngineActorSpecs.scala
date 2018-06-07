package hicoden.jobgraph.engine


import hicoden.jobgraph.{Job, WorkflowId, Workflow}

import scala.concurrent.duration._
import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.pattern._
import akka.actor._
import akka.testkit._
import org.scalatest.{ BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike }

class EngineActorSpecs() extends TestKit(ActorSystem("EngineActorSpecs")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "Engine" must {

    "not proceed to execute if the workflow id is not found; when no jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], Nil, Nil), "Engine-1")
      val nonExistentId = 42

      engine ! StartWorkflow(nonExistentId)
      expectMsg("No such id")
    }

    "not proceed to execute if the workflow id is not found; when jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-2")
      val existentId = 1
      engine ! StartWorkflow(existentId)
      expectMsg("No such id")
    }

    "proceed to execute if the workflow id is found after jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-3")
      val existentId = 18
      within(3 second) {
        engine ! StartWorkflow(existentId)
        Thread.sleep(2000)
        expectMsgType[String] // in truth, the runtime generated workflow-id (in UUID format) is generated and returned.
      }
    }

    "proceed to execute concurrently if the workflow id is found after jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-4")
      val existentId = 19
      within(10 second) {
        engine ! StartWorkflow(existentId)
        Thread.sleep(4000)
        expectMsgType[String] // in truth, the runtime generated workflow-id (in UUID format) is generated and returned.
      }

    }

    "not proceed to execute if the workflow id is found but there's a loop; when jobs nor workflows are loaded." in {
      val engine = system.actorOf(Props(classOf[Engine], "jobs_for_engine_actor_specs":: Nil, "workflows_for_engine_actor_specs" :: Nil), "Engine-5")
      val existentId = 20
      engine ! StartWorkflow(existentId)
      Thread.sleep(2000)
      expectMsgType[String]
    }

  }

}

