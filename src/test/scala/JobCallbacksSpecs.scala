package hicoden.jobgraph.engine

import org.specs2.mutable.Specification
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.Specs2RouteTest
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.server._
import StatusCodes._
import Directives._

import scala.concurrent.duration._

//
// Reason why we need 2 spec files i/o 1 is because of the [[engine]]
// reference.
//
class JobCallbacksSpecs extends Specification with Specs2RouteTest with JobCallbacks {

  val actorSystem = system
  val actorMaterializer = materializer

  // engine is given a null value because we want to simulate the absense of
  // the engine
  val engine = akka.actor.Actor.noSender

  "The call back to cancel Google Dataflow jobs" should {

    "return a non-HTTP-200 code when workflow is a UUID, engine is NOT available and workflowId is valid." in {
      val wfId = java.util.UUID.randomUUID
      Post(s"/flows/$wfId/cancel") ~> JobCallbackRoutes ~> check {
        status shouldEqual InternalServerError
        responseAs[String] shouldEqual s"There was an internal server error."
      }
    }

    "return a non-HTTP-200 code when workflow is not a UUID, engine is NOT available." in {
      val wfId = "not-a-uuid"
      Post(s"/flows/$wfId/cancel") ~> JobCallbackRoutes ~> check {
        status shouldEqual InternalServerError
        responseAs[String] shouldEqual s"There was an internal server error."
      }
    }

  }

  "The call back by executing jobs" should {

    "return a non HTTP-200 code when workflow and job ids are UUIDs but engine is no longer available." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      Post(s"/flows/$wfId/job/$jobId") ~> JobCallbackRoutes ~> check {
        responseAs[String] shouldEqual "Request does not contain valid JSON"
      }
    }

    "return a non HTTP-200 code when workflow and job ids are not UUIDs regardless of whether engine is available." in {
      val wfId = "not-a-uuid"
      val jobId = "not-a-uuid"
      Post(s"/flows/$wfId/job/$jobId") ~> JobCallbackRoutes ~> check {
        responseAs[String] shouldEqual "There was an internal server error."
      }
    }

  }

}

//
// The purpose of this [[Echo]] is really to return the datum passed to it
// and its injected for this series of tests
//
class Echo extends akka.actor.Actor {
  def receive = {
    case msg ⇒
  }
}

class JobCallbacksSpecs2 extends Specification with Specs2RouteTest with JobCallbacks {

  val actorSystem = system
  val actorMaterializer = materializer

  // engine is given a reference to the [[Echo]] actor here because we want to
  // capture the callbacks.
  val engine = actorSystem.actorOf(akka.actor.Props[Echo])

  "The call back to cancel Google Dataflow jobs" should {

    "return a HTTP-200 code when workflow is a UUID, engine is available and workflowId is valid." in {
      val wfId = java.util.UUID.randomUUID
      Post(s"/flows/$wfId/cancel") ~> JobCallbackRoutes ~> check {
        responseAs[String] shouldEqual s"OK. Engine will stop Dataflow for workflow-id: $wfId"
      }
    }

    "return a non-HTTP-200 code when workflow is not a UUID, engine is available." in {
      val wfId = "not-a-uuid"
      Post(s"/flows/$wfId/cancel") ~> JobCallbackRoutes ~> check {
        status shouldEqual InternalServerError
        responseAs[String] shouldEqual s"There was an internal server error."
      }
    }

  }

  "The call back to begin monitoring of the job" should {

    "return a HTTP-200 code when workflow and job ids are UUIDs and engine is available but no json payload is detected." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      Post(s"/flows/$wfId/job/$jobId") ~> JobCallbackRoutes ~> check {
        responseAs[String] shouldEqual s"Request does not contain valid JSON"
      }
    }

    "return a HTTP-200 code when workflow and job ids are UUIDs and engine is available and expected json payload is parsed and validated." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      import io.circe._
      val data = """ {"google_dataflow_id" : "hi"} """

      Post(s"/flows/$wfId/job/$jobId", HttpEntity(`application/json`, data)) ~> JobCallbackRoutes ~> check {
        status shouldEqual OK
        responseAs[String] must startWith(s"OK. Engine will supervise Dataflow jobId: $jobId")
      }
    }

    "return a HTTP-200 code when workflow and job ids are UUIDs and engine is available and expected json payload has the correct key but wrong value type." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      import io.circe._
      val data = """ {"google_dataflow_id" : 42 } """ // expected value type should be string instead of integers

      Post(s"/flows/$wfId/job/$jobId", HttpEntity(`application/json`, data)) ~> JobCallbackRoutes ~> check {
        status shouldEqual OK
        responseAs[String] must startWith(s"Either we did not see the key")
      }
    }

    "return a HTTP-200 code when workflow and job ids are UUIDs and engine is available and expected json payload does not contain the correct key." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      import io.circe._
      val data = """ {"google_dataflow_id_wrong" : "i dont care what value is here" } """ // expected value type should be string instead of integers

      Post(s"/flows/$wfId/job/$jobId", HttpEntity(`application/json`, data)) ~> JobCallbackRoutes ~> check {
        status shouldEqual OK
        responseAs[String] must startWith(s"Either we did not see the key")
      }
    }

  }

}

