package hicoden.jobgraph.engine

import akka.actor._
import hicoden.jobgraph.fsm.runners.{DataflowJobTerminationRunner, DataflowTerminationContext}
import hicoden.jobgraph.fsm.runners.runtime.GoogleDataflowFunctions
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}

case class WhatToStop(googleDataflowJobs : List[GoogleDataflowId])

//
// This carries out the work of cancelling the Google Dataflow jobs
// What it does in detail is that it would look for a script and activate that
// script to perform the actual cancellation by passing it a string of job-ids
// (space separated)
//
class DataflowJobTerminator extends Actor with ActorLogging {

  def receive = {

    case WhatToStop(googleDataflowJobs) ⇒
      val ctx : DataflowTerminationContext =
        DataflowTerminationContext(
          getClass.getClassLoader.getResource("gcloud_cancel_job.sh").getPath.toString :: Nil,
          googleDataflowJobs) /* this produces a nice string which will be consumed by the program , this is mandatory */

      val runner = new DataflowJobTerminationRunner
      sender ! GoogleDataflowFunctions.interpretCancelJobResult(runner.run(ctx)(id ⇒ id))

  }

}


