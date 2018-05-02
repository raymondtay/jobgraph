package hicoden.jobgraph.engine

import hicoden.jobgraph.{Workflow, WorkflowId}
import akka.actor.ActorRef

/**
  * State FSM of each Engine. Each [[Engine]] actor, when started, will have an
  * empty "state" and this is the state abstraction that the engine will use
  * when wanting to keep track of what goes on in the asynchronous processing.
  *
  * @author Raymond Tay
  * @version 1.0
  */

trait EngineStateOps {

  import cats._, data._, implicits._

  /**
    * Obtains the Workflow ADT that holds 'active' instances
    * @param a workflow ADT of type [[WFA]]
    * @return the workflow ADT of type [[WFA]]
    */
  def getCurrentActiveWorkflows : State[WFA, WFA] = for { s ← State.get[WFA] } yield s

  /**
    * Inserts the workflow (and associated workers) to the Workflow ADT and
    * returns the resultant ADT.
    * @param wfId workflow id
    * @param workers the set of references to actors
    * @return the workflow ADT of type [[WFA]]
    */
  def addToActive(workflowId: WorkflowId) : Kleisli[State[WFA, ?], Set[ActorRef], WFA] =
    Kleisli{ (workers: Set[ActorRef]) ⇒
      for {
        s  ← State.get[WFA]
        _  ← State.modify((active: WFA) ⇒ active += workflowId -> workers)
        s2 ← State.get[WFA]
      } yield s2
    }

   /**
    * Removes the workflow (and associated workers) from the Workflow ADT and
    * returns the resultant ADT.
    * @param wfId workflow id
    * @return the workflow ADT of type [[WFA]]
    */
  def removeFromActive : Kleisli[State[WFA, ?], WorkflowId, Boolean] =
    Kleisli{ (workflowId: WorkflowId) ⇒
      for {
        s  ← State.get[WFA]
        _  ← State.modify((active: WFA) ⇒ active -= workflowId)
        s2 ← State.get[WFA]
      } yield s2.contains(workflowId)
    }

   /**
    * Updates the workflow (and associated workers with the passed-in workers) from the Workflow ADT and
    * returns the resultant ADT.
    * @param wfId workflow id
    * @param workers set of actor references to be replaced
    * @return the workflow ADT of type [[WFA]]
    */
  def updateActive(workflowId: WorkflowId) : Kleisli[State[WFA, ?], Set[ActorRef], WFA] =
    Kleisli{ (workers: Set[ActorRef]) ⇒
      for {
        s  ← State.get[WFA]
        _  ← State.modify{(active: WFA) ⇒ 
                             active -= workflowId
                             active += workflowId -> workers
                         }
        s2 ← State.get[WFA]
      } yield s2
    } 

}
