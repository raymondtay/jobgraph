package hicoden.jobgraph.engine

import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import akka.pattern._
import akka.testkit.{ ImplicitSender, TestActors, TestKit }
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util._
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpecLike }

//
// The specs here are pretty straight-forward w.r.t Google Gloud SDK as the
// returns, followed by a successful invocation to Google, is a series of
// strings. That's also pretty fragile as we are subjected to changes on their
// SDK.
//
class DataflowJobTerminatorActorSpec() extends TestKit(ActorSystem("DataflowJobTerminatorActorSpec")) with
  ImplicitSender with
  WordSpecLike with
  Matchers with
  BeforeAndAfterAll {


  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "An DataflowJobTerminator" must {

    "be able to send a termination signal to Google Dataflow SDK." in {
      val a = system.actorOf(Props[DataflowJobTerminator])
      a ! WhatToStop(Nil)
      expectMsg(Some(List("Cancelled job []")))
    }

    "be able to send a termination signal to Google Dataflow SDK and the return message can be validated." in {
      val actor = system.actorOf(Props[DataflowJobTerminator])
      implicit val timeOut = akka.util.Timeout(1 second)
      implicit val ex = system.dispatcher
      val fakeJobId1 = "fake-google-job-id"
      val fakeJobId2 = "fake-google-job-id"
      actor ! WhatToStop(List(fakeJobId1, fakeJobId2))
      expectMsg(Some(List(s"Cancelled job [$fakeJobId1 $fakeJobId2]")))
    }

  }

}

