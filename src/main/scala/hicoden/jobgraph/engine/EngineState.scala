package hicoden.jobgraph.engine

import hicoden.jobgraph.{Job, JobId, Workflow, WorkflowId}
import hicoden.jobgraph.configuration.step.model.JobConfig
import hicoden.jobgraph.configuration.workflow.model.{JobConfigOverrides, WorkflowConfig}
import hicoden.jobgraph.configuration.step.JobDescriptorTable
import hicoden.jobgraph.configuration.workflow.WorkflowDescriptorTable
import akka.actor.{ActorRef, ActorPath}

/**
  * State FSM of each Engine. Each [[Engine]] actor, when started, will have an
  * empty "state" and this is the state abstraction that the engine will use
  * when wanting to keep track of what goes on in the asynchronous processing.
  *
  * @author Raymond Tay
  * @version 1.0
  */

//
// ADTs needed by JobGraph
//
case class ActiveWorkflows(map: WFA)
case class FailedWorkflows(map: WFA)
case class WorkersToWorkflow(map: Map[ActorPath, WorkflowId]) 
case class ActiveGoogleDataflow(map : Map[GoogleDataflowId, WorkflowId])
case class JobDescriptors(map: JobDescriptorTable)
case class WorkflowDescriptors(map : WorkflowDescriptorTable)

// Functions for managing the state of the queues
trait EngineQueueStateOps {

  import cats._, data._, implicits._

  //
  // NOTE: DO NOT MANIPULATE THE FOLLOWING ADTS DIRECTLY !!!
  //       Do use the State Monad functions, augment your own if you need to 
  //
  //



  /* Mapping of executing workflows Workflow Id -> (Mapping of async workers to a job) */
  var ACTIVE_WORKFLOWS : ActiveWorkflows = _

  /* Mapping of failed workflows Workflow Id -> (Mapping of async workers to a job) */
  var FAILED_WORKFLOWS : FailedWorkflows = _

  /* Mapping of async workers to their workflows by associating by Workflow Id */
  var WORKERS_TO_WF_LOOKUP : WorkersToWorkflow = _

  /* Mapping of google dataflow id -> workflow id */
  var ACTIVE_DATAFLOW_JOBS : ActiveGoogleDataflow = _

  /**
    * Updates the internal ADTs to reflect the state of both the
    * ActiveWorkflows and WorkersToWorkflow; underneath its really 'bimap'
    * @param pair 2-tuple where (active, reverse-lookup of active)
    * @return A Left(<error>) or a Right(2-tuple)
    */
  def updateActiveNLookupTableState : Reader[(ActiveWorkflows, WorkersToWorkflow), Either[_, (ActiveWorkflows, WorkersToWorkflow)]] =
    Reader { (p: (ActiveWorkflows, WorkersToWorkflow)) ⇒
      p.bimap(l ⇒ setActiveWorkflows(l.map), r ⇒ setWorkersToWorkflow(r.map)).bimap(_.runS(p._1).value, _.runS(p._2).value).asRight
    }

  // State function that consumes the state 'ActiveWorkflows' and sets the
  // datum to it
  private def setActiveWorkflows(datum : WFA) : State[ActiveWorkflows, Boolean] =
    for {
      x  ← State.get[ActiveWorkflows]
      _  ← State.modify((s: ActiveWorkflows) ⇒ s.copy(map = datum))
      s2 ← State.get[ActiveWorkflows]
    } yield {
      ACTIVE_WORKFLOWS = s2
      true
    }

  // State function that consumes the state 'WorkersToWorkflow' and sets the
  // datum to it
  private def setWorkersToWorkflow(datum : Map[ActorPath, WorkflowId]) : State[WorkersToWorkflow, Boolean] =
    for {
      x  ← State.get[WorkersToWorkflow]
      _  ← State.modify((s: WorkersToWorkflow) ⇒ s.copy(map = datum))
      s2 ← State.get[WorkersToWorkflow]
    } yield {
      WORKERS_TO_WF_LOOKUP = s2
      true
    }

  def addToActive2(workflowId: WorkflowId) : Kleisli[State[ActiveWorkflows, ?], Set[(ActorRef,Job)], Boolean] =
    Kleisli{ (workers: Set[(ActorRef, Job)]) ⇒
      for {
        s  ← State.get[ActiveWorkflows]
        _  ← State.modify((active: ActiveWorkflows) ⇒ active.copy(map = active.map += workflowId → workers.toMap))
        s2 ← State.get[ActiveWorkflows]
      } yield {
        ACTIVE_WORKFLOWS = s2
        true
      }
    }

  /**
    * Add to the lookup table
    * @param wfId
    * @param workersToWorkflow
    * @return State object
    */
  def addToLookup2(wfId: WorkflowId) : Reader[Set[(ActorRef,Job)], State[WorkersToWorkflow,_]] =
    Reader{ (actorRefs: Set[(ActorRef,Job)]) ⇒
      for {
        s ← State.get[WorkersToWorkflow]
        _ ← State.modify{(lookupT: WorkersToWorkflow) ⇒
                             var mutM = collection.mutable.Map(lookupT.map.toList: _*)
                             actorRefs.collect{case pair ⇒ pair._1}.toList.map(ref ⇒ mutM += (ref.path → wfId))
                             lookupT.copy(map = collection.immutable.Map(mutM.toList: _*))
                         }
        s2 ← State.get[WorkersToWorkflow]
      } yield {
        WORKERS_TO_WF_LOOKUP = s2
        true
      }
    }

  /**
    * Function that attempts to look for a match by looking at the state data
    * @param workflowId
    * @return State[ActiveWorkflows, Boolean]
    */
  def removeActiveWorkflowsBy : Reader[WorkflowId, State[ActiveWorkflows, Option[Map[ActorRef,Job]]]] =
    Reader { (workflowId: WorkflowId) ⇒
      for {
        s  ← State.get[ActiveWorkflows]
        _  ← State.modify((m: ActiveWorkflows) ⇒ m.copy(map = m.map - workflowId))
        s2 ← State.get[ActiveWorkflows]
      } yield s.map.get(workflowId)
    }

  private
  def storeToFailedWorkflowsBy(workflowId: WorkflowId) : Reader[Map[ActorRef,Job], State[FailedWorkflows, Boolean]] =
    Reader { (workers: Map[ActorRef,Job]) ⇒
      for {
        x  ← State.get[FailedWorkflows]
        _  ← State.modify((s: FailedWorkflows) ⇒ s.copy(map = s.map + (workflowId → workers)))
        s2 ← State.get[FailedWorkflows]
      } yield {
        FAILED_WORKFLOWS = s2
        true
      }
    }

  private
  def getAllWorkersToWorkflowBy : Reader[ActorPath, State[WorkersToWorkflow, Boolean]] =
    Reader { (fqPathToWorker: ActorPath) ⇒
      for {
        s ← State.get[WorkersToWorkflow]
      } yield s.map.contains(fqPathToWorker)
    }

  /**
    * Discover if the workflowId is present in the given State object
    * @param workflowId
    * @param active workflows
    * @return State object
    */
  def isWorkflowInActiveWorkflows : Reader[WorkflowId, State[ActiveWorkflows, Boolean]] =
    Reader { (workflowId: WorkflowId) ⇒
      for {
        s ← State.get[ActiveWorkflows]
      } yield s.map.contains(workflowId)
    }

  /**
    * Attempts to find the workflow from the active storage, removes it and
    * places it to the failed storage
    * @param active
    * @param failed
    * @param wfId
    * @return a 2-tuple where (failed + wfId, active - wfId)
    */
  def dropWorkflowFromActiveToFailedBy : Reader[WorkflowId, Boolean] =
    Reader { (workflowId: WorkflowId) ⇒
      removeActiveWorkflowsBy(workflowId).runA(ACTIVE_WORKFLOWS).value.fold(false)
      {(workflow: Map[ActorRef,Job]) ⇒ storeToFailedWorkflowsBy(workflowId)(workflow).runA(FAILED_WORKFLOWS).value}
    }

  /**
    * Lookup all workers for a particular workflow
    * @param wfId
    * @return Something or Nothing
    */
  def lookupWorkersFromActive : Reader[WorkflowId, State[ActiveWorkflows, Option[Map[ActorRef,Job]]]] =
    Reader { (wfId: WorkflowId) ⇒
      for {
        s ← State.get[ActiveWorkflows]
      } yield s.map.get(wfId).fold(none[Map[ActorRef,Job]])(_.some)
    }

  /**
    * Lookup specific worker by matching the workflow-id and job-id for a particular workflow
    * @param wfId
    * @param jobId
    * @return Something or Nothing
    */
  def lookupWorkerFromActive(wfId: WorkflowId) : Reader[JobId, State[ActiveWorkflows, Option[(ActorRef,Job)]]] =
    Reader { (jobId: JobId) ⇒
      for {
        s ← State.get[ActiveWorkflows]
      } yield s.map.get(wfId).fold(none[(ActorRef,Job)])(m ⇒ m.find( (p: (ActorRef,Job)) ⇒ p._2.id equals jobId ))
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
  def bindDataflowToWorkflow(wfId: WorkflowId) : Reader[GoogleDataflowId, State[ActiveGoogleDataflow,Boolean]] =
    Reader{ (dataflowId: String) ⇒
      for {
        s  ← State.get[ActiveGoogleDataflow]
        _  ← State.modify((m: ActiveGoogleDataflow) ⇒ m.copy(map = m.map + (dataflowId → wfId)))
        s2 ← State.get[ActiveGoogleDataflow]
      } yield {
        ACTIVE_DATAFLOW_JOBS = s2
        true
      }
    }

   /**
    * Removes the workflow (and specific worker by matching job id) from the Workflow ADT and
    * returns the resultant ADT.
    * @param wfId workflow id
    * @param jobId job id
    * @return the workflow ADT of type [[WFA]]
    */
  def removeJobFromActive(workflowId: WorkflowId) : Kleisli[State[ActiveWorkflows, ?], WorkflowId, Boolean] =
    Kleisli{ (jobId: JobId) ⇒
      for {
        s  ← State.get[ActiveWorkflows]
        _  ← State.modify{(active: ActiveWorkflows) ⇒ 
               (for {
                 m ← collection.mutable.Map(active.map.toSeq:_*).get(workflowId)
                 n ← m.find((p:(ActorRef,Job)) ⇒ p._2.id equals jobId)
               } yield m - n._1).fold(active)(m ⇒ active.copy(map = collection.mutable.Map(workflowId -> Map(m.toSeq:_*))))
            }
        s2 ← State.get[ActiveWorkflows]
      } yield {
        ACTIVE_WORKFLOWS = s2
        !s2.map.contains(workflowId)
      }
    }

  /**
    * Lookup the binding between workflow -> (dataflow-id1, dataflow-id2, ...)
    * @param wfId
    * @param state
    * @return a container with the bounded dataflow-ids or an empty container
    */
  def lookupDataflowBindings : Reader[WorkflowId, State[ActiveGoogleDataflow, List[GoogleDataflowId]]] =
    Reader { (wfId: WorkflowId) ⇒
      for {
        s ← State.get[ActiveGoogleDataflow]
      } yield s.map.collect{ case (k, v) if (v equals wfId) ⇒ k }.toList
    }

  /**
    * Remove the binding for the matching workflow id 
    * @param wfId
    * @return state object
    */
  def removeFromDataflowBindings : Reader[WorkflowId, State[ActiveGoogleDataflow, Boolean]] =
    Reader { (wfId: WorkflowId) ⇒
      for {
        s  ← State.get[ActiveGoogleDataflow]
        _  ← State.modify((m: ActiveGoogleDataflow) ⇒ m.copy(map = m.map.filter(kv ⇒ kv._2 != wfId)))
        s2 ← State.get[ActiveGoogleDataflow]
      } yield {
        ACTIVE_DATAFLOW_JOBS = s2
        true
      }
    }

  /**
    * Remove [[actor]] from the lookup table and returns a 2-tuple where the
    * 1st element is workflow-id this belongs to, 2nd element is the updated
    * state.
    * @param actor
    * @param state
    * @return 2-tuple
    */
  def removeFromLookup : Reader[ActorRef, State[WorkersToWorkflow, WorkflowId]] =
    Reader{ (actor: ActorRef) ⇒
      for {
        s  ← State.get[WorkersToWorkflow]
        _  ← State.modify((lookupT: WorkersToWorkflow) ⇒ lookupT.copy(map = lookupT.map - actor.path))
        s2 ← State.get[WorkersToWorkflow]
      } yield {
        WORKERS_TO_WF_LOOKUP = s2
        s.map(actor.path)
      }
    }

}

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
