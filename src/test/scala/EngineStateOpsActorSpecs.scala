package hicoden.jobgraph.engine


import hicoden.jobgraph.{Job, WorkflowId, Workflow}

import akka.actor.{ActorSystem,ActorPath,ActorRef}
import akka.testkit.{ ImplicitSender, TestActors, TestKit }
import org.scalatest.{ BeforeAndAfterEach, BeforeAndAfterAll, Matchers, WordSpecLike }

class EngineStateOpsActorSpecs() extends TestKit(ActorSystem("EngineStateOpsActorSpecs")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterEach with BeforeAndAfterAll with EngineStateOps2 {

  override def beforeEach {
    ACTIVE_WORKFLOWS     = ActiveWorkflows(collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]])
    FAILED_WORKFLOWS     = FailedWorkflows(collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]])
    WORKERS_TO_WF_LOOKUP = WorkersToWorkflow(collection.mutable.Map.empty[ActorPath, WorkflowId])
    ACTIVE_DATAFLOW_JOBS = ActiveGoogleDataflow(collection.mutable.Map.empty[GoogleDataflowId, WorkflowId])
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "EngineStateOps" must {

    "Add operation: Maintain mapping for 1 mapping" in {
      val echo = system.actorOf(TestActors.echoActorProps)
      val echoJob = Job("echo")
      val workflowId = java.util.UUID.randomUUID
      addToLookup(workflowId)(Set((echo, echoJob))).runS(WORKERS_TO_WF_LOOKUP).value.map.contains(echo.path) == true
    }

    "Add operation: Maintain mapping for 2 mapping" in {
      val echo1 = system.actorOf(TestActors.echoActorProps)
      val echo2 = system.actorOf(TestActors.echoActorProps)
      val echoJob1 = Job("echo1")
      val echoJob2 = Job("echo2")
      val workflowId = java.util.UUID.randomUUID
      addToLookup(workflowId)(Set( (echo1, echoJob1), (echo2, echoJob2))).runS(WORKERS_TO_WF_LOOKUP).value
      (WORKERS_TO_WF_LOOKUP.map.contains(echo1.path) &&
      WORKERS_TO_WF_LOOKUP.map.contains(echo2.path)) == true
    }

    "Add and Remove operation: Maintain mapping for 2 mapping" in {
      val echo1 = system.actorOf(TestActors.echoActorProps)
      val echo2 = system.actorOf(TestActors.echoActorProps)
      val echo3 = system.actorOf(TestActors.echoActorProps)
      val echoJob1 = Job("echo1")
      val echoJob2 = Job("echo2")
      val echoJob3 = Job("echo3")
      val workflowId = java.util.UUID.randomUUID
      addToLookup(workflowId)(Set( (echo1, echoJob1), (echo2, echoJob2), (echo3, echoJob3))).runS(WORKERS_TO_WF_LOOKUP).value
      (WORKERS_TO_WF_LOOKUP.map.contains(echo1.path) &&
       WORKERS_TO_WF_LOOKUP.map.contains(echo2.path) &&
       WORKERS_TO_WF_LOOKUP.map.contains(echo3.path)) == true
      removeFromLookup(echo2).runS(WORKERS_TO_WF_LOOKUP).value
      WORKERS_TO_WF_LOOKUP.map.contains(echo2.path) == false
    }

  }
}

