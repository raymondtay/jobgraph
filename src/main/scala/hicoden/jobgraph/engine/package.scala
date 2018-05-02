package hicoden.jobgraph

import akka.actor.ActorRef

package object engine {

  type WFA = collection.mutable.Map[WorkflowId, Set[ActorRef]]
  type WFI = collection.mutable.Map[WorkflowId, Workflow]

}
