package hicoden.jobgraph.engine


import hicoden.jobgraph.{WorkflowId, Workflow}

import akka.actor.{ActorSystem,ActorPath}
import akka.testkit.{ ImplicitSender, TestActors, TestKit }
import org.scalatest.{ BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike }

class EngineStateOpsActorSpecs() extends TestKit(ActorSystem("EngineStateOpsActorSpecs")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with EngineStateOps {

  var state : scala.collection.mutable.Map[ActorPath, WorkflowId] = null

  override def beforeEach {
    state = scala.collection.mutable.Map.empty[ActorPath, WorkflowId]
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "EngineStateOps" must {

    "Add operation: Maintain mapping for 1 mapping" in {
      val echo = system.actorOf(TestActors.echoActorProps)
      val workflowId = java.util.UUID.randomUUID
      addToLookup(workflowId)(Set(echo)).runS(state).value.contains(echo.path) == true
    }

    "Add operation: Maintain mapping for 2 mapping" in {
      val echo1 = system.actorOf(TestActors.echoActorProps)
      val echo2 = system.actorOf(TestActors.echoActorProps)
      val workflowId = java.util.UUID.randomUUID
      state = addToLookup(workflowId)(Set(echo1, echo2)).runS(state).value
      state.contains(echo1.path) == true
      state.contains(echo2.path) == true
    }

    "Add and Remove operation: Maintain mapping for 2 mapping" in {
      val echo1 = system.actorOf(TestActors.echoActorProps)
      val echo2 = system.actorOf(TestActors.echoActorProps)
      val echo3 = system.actorOf(TestActors.echoActorProps)
      val workflowId = java.util.UUID.randomUUID
      state = addToLookup(workflowId)(Set(echo1, echo2, echo3)).runS(state).value
      state.contains(echo1.path) == true
      state.contains(echo2.path) == true
      state.contains(echo3.path) == true
      removeFromLookup(echo2).runS(state).value
      state.contains(echo2.path) == false
    }

  }
}

