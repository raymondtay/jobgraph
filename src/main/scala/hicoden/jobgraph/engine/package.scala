package hicoden.jobgraph

import hicoden.jobgraph.configuration.step.model._
import scala.language.{postfixOps, implicitConversions}
import akka.actor._
import akka.pattern._
import scala.concurrent.duration._

package object engine {

  type WFA = collection.mutable.Map[WorkflowId, Map[ActorRef, Job]]
  type WFI = collection.mutable.Map[WorkflowId, Workflow]
  type ActorRefString = String
  type GoogleDataflowId = String

  implicit def mutableMapToImmute[A,B](mutM: collection.mutable.Map[A,B]) = collection.immutable.Map(mutM.toList: _*)
  implicit def immutableMapToMutable[A,B](m: collection.immutable.Map[A,B]) = collection.mutable.Map(m.toList: _*)

  // The back-off strategy here is applicable that applies to general failures
  // from the [[JobFSM]] and it can be classified into recoverable exceptions
  // and non-recoverable exceptions (the latter typically indicates that
  // there's some kind of runtime or logical error)
  //
  // With respects to the processing of the Beam pipeline via Dataflow, we are
  // only restarting the job iff we see a condition like
  // [[PipelineResult.FAILED]] see [[FSM.scala]] for details
  //
  def backoffOnFailureStrategy(childProps: Props, childName : String, jobConfig : JobConfig) = {
    val options =
      Backoff.
        onFailure(childProps,
                  childName,
                  minBackoff = 200 millis,
                  maxBackoff = 10 seconds,
                  randomFactor = 0.2).
        withSupervisorStrategy(
          OneForOneStrategy(maxNrOfRetries = jobConfig.restart.max) {
            case _: NullPointerException         ⇒ SupervisorStrategy.Stop
            case _: DataflowRestartableException ⇒ SupervisorStrategy.Restart
            case x                               ⇒ SupervisorStrategy.Escalate
      })
    BackoffSupervisor.props(options)
  }

  class DataflowRestartableException(msg: String) extends RuntimeException(msg)
  class DataflowFailure extends DataflowRestartableException("Dataflow job has failed")
}

