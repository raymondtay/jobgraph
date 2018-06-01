package hicoden.jobgraph.engine

import hicoden.jobgraph.{Job, JobId, Workflow, WorkflowId}
import akka.actor.{ActorRef, ActorPath}

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
    * Add to the lookup table
    * @param wfId
    * @param actorRefs
    * @param state
    * @return updated state
    */
  def addToLookup(wfId: WorkflowId) : Reader[Set[(ActorRef,Job)], State[Map[ActorPath,WorkflowId],_]] = Reader{ (actorRefs: Set[(ActorRef,Job)]) ⇒
    for {
      s ← State.get[Map[ActorPath,WorkflowId]]
      _ ← State.modify{(lookupT: Map[ActorPath, WorkflowId]) ⇒
                           var mutM = collection.mutable.Map(lookupT.toList: _*)
                           actorRefs.collect{case pair ⇒ pair._1}.toList.map(ref ⇒ mutM += (ref.path → wfId))
                           collection.immutable.Map(mutM.toList: _*)
                       }
      s2 ← State.get[Map[ActorPath,WorkflowId]]
    } yield s2
  }

  /**
   * Lookup the FSM worker handling the job in question.
   * @param wfId
   * @param jobId
   * @param state the active workflows
   * @return updated state
   */
  def lookupActive(wfId: WorkflowId) : Reader[JobId, State[WFA, Option[(ActorRef,Job)]]] = Reader { (jobId: JobId) ⇒
    for {
      s ← State.get[WFA]
    } yield s.get(wfId).fold(none[(ActorRef,Job)])(m ⇒ m.find( (p: (ActorRef,Job)) ⇒ p._2.id equals jobId ))
  }

  /**
    * Creates the mapping between the Google Dataflow id and the workflow in
    * question. The use case is to launch workers to issue a [[cancel]] or
    * [[drain]] command
    * @param wfId
    * @param dataflowId google issued id e.g. 2018-05-31_20_13_42-3072150195405820451
    * @param state
    * @return the modified state
    */
  def bindDataflowToWorkflow(wfId: WorkflowId) = Reader{ (dataflowId: String) ⇒
    for {
      s  ← State.get[Map[GoogleDataflowId, WorkflowId]]
      _  ← State.modify((m: Map[GoogleDataflowId, WorkflowId]) ⇒ m + (dataflowId → wfId))
      s2 ← State.get[Map[GoogleDataflowId, WorkflowId]]
    } yield s2
  }

  /**
    * Lookup the binding between workflow -> (dataflow-id1, dataflow-id2, ...)
    * @param wfId
    * @param state
    * @return a container with the bounded dataflow-ids or an empty container
    */
  def lookupDataflowBindings : Reader[WorkflowId, State[Map[GoogleDataflowId,WorkflowId], List[GoogleDataflowId]]] =
    Reader { (wfId: WorkflowId) ⇒
      for {
        s ← State.get[Map[GoogleDataflowId, WorkflowId]]
      } yield s.collect{ case (k, v) if (v equals wfId) ⇒ k }.toList
    }

  def removeFromDataflowBindings = Reader{ (wfId: WorkflowId) ⇒
    for {
      s  ← State.get[Map[GoogleDataflowId, WorkflowId]]
      _  ← State.modify((m: Map[GoogleDataflowId, WorkflowId]) ⇒ m.filter(kv ⇒ kv._2 != wfId))
      s2 ← State.get[Map[GoogleDataflowId, WorkflowId]]
    } yield s2
  }

  /**
    * Remove [[actor]] from the lookup table and returns a 2-tuple where the
    * 1st element is workflow-id this belongs to, 2nd element is the updated
    * state.
    * @param actor
    * @param state
    * @return 2-tuple
    */
  def removeFromLookup = Reader{ (actor: ActorRef) ⇒
    for {
      s  ← State.get[Map[ActorPath,WorkflowId]]
      _  ← State.modify((lookupT: Map[ActorPath, WorkflowId]) ⇒ lookupT - actor.path)
      s2 ← State.get[Map[ActorPath,WorkflowId]]
    } yield (s(actor.path), s2)
  }

  /**
    * Inserts the workflow (and associated workers) to the Workflow ADT and
    * returns the resultant ADT.
    * @param wfId workflow id
    * @param workers the set of references to actors
    * @return the workflow ADT of type [[WFA]]
    */
  def addToActive(workflowId: WorkflowId) : Kleisli[State[WFA, ?], Set[(ActorRef,Job)], WFA] =
    Kleisli{ (workers: Set[(ActorRef, Job)]) ⇒
      for {
        s  ← State.get[WFA]
        _  ← State.modify((active: WFA) ⇒ active += workflowId → workers.toMap)
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
  def updateActive(workflowId: WorkflowId) : Kleisli[State[WFA, ?], Set[(ActorRef,Job)], WFA] =
    Kleisli{ (workers: Set[(ActorRef, Job)]) ⇒
      for {
        s  ← State.get[WFA]
        _  ← State.modify{(active: WFA) ⇒ 
                             active -= workflowId
                             active += workflowId → workers.toMap
                         }
        s2 ← State.get[WFA]
      } yield s2
    } 

}
