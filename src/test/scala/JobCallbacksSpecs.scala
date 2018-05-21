package hicoden.jobgraph.engine

import org.specs2.mutable.Specification
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.Specs2RouteTest
import akka.http.scaladsl.server._
import Directives._

import scala.concurrent.duration._

//
// Reason why we need 2 spec files i/o 1 is because of the [[engine]]
// reference.
//
class JobCallbacksSpecs extends Specification with Specs2RouteTest with JobCallbacks {

  val actorSystem = system
  val actorMaterializer = materializer
  val engine = akka.actor.Actor.noSender

  "The call back by executing jobs" should {

    "return a non HTTP-200 code when workflow and job ids are UUIDs but engine is no longer available." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      Post(s"/flow/$wfId/job/$jobId") ~> route ~> check {
        responseAs[String] shouldEqual "There was an internal server error."
      }
    }

    "return a non HTTP-200 code when workflow and job ids are not UUIDs regardless of whether engine is available." in {
      val wfId = "not-a-uuid"
      val jobId = "not-a-uuid"
      Post(s"/flow/$wfId/job/$jobId") ~> route ~> check {
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
    case msg ⇒ sender ! msg
  }
}

class JobCallbacksSpecs2 extends Specification with Specs2RouteTest with JobCallbacks {

  val actorSystem = system
  val actorMaterializer = materializer
  val engine = actorSystem.actorOf(akka.actor.Props[Echo])

  "The call back by executing jobs" should {

    "return a HTTP-200 code when workflow and job ids are UUIDs and engine is available." in {
      val wfId = java.util.UUID.randomUUID
      val jobId = java.util.UUID.randomUUID
      Post(s"/flow/$wfId/job/$jobId") ~> route ~> check {
        responseAs[String] shouldEqual s"OK. Engine will supervise Dataflow jobId: $jobId"
      }
    }

  }

}

