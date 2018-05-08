package hicoden.jobgraph

import scala.language.implicitConversions
import akka.actor.ActorRef

package object engine {

  type WFA = collection.mutable.Map[WorkflowId, Set[ActorRef]]
  type WFI = collection.mutable.Map[WorkflowId, Workflow]
  type ActorRefString = String

  implicit def mutableMapToImmute[A,B](mutM: collection.mutable.Map[A,B]) = collection.immutable.Map(mutM.toList: _*)
  implicit def immutableMapToMutable[A,B](m: collection.immutable.Map[A,B]) = collection.mutable.Map(m.toList: _*)

}
