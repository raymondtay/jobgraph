package hicoden.jobgraph.engine

import hicoden.jobgraph.{Job, WorkflowId, Workflow}

import akka.actor._
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary._
import Gen._
import Prop.{forAll, throws, AnyOperators}

//
// The type JobDescriptors is the typed synonym for active workflows
//
object EngineStateOpsData {

  def emptyActiveWF = ActiveWorkflows(scala.collection.mutable.Map.empty[WorkflowId, Map[ActorRef, Job]])

  def genEmptyWFA : Gen[ActiveWorkflows] = oneOf(emptyActiveWF :: Nil)

  def genDataflows = for {
    googleDataflowId ← uuid
    workflowId       ← uuid
  } yield (googleDataflowId.toString, workflowId)

  def emptyLookup = WorkersToWorkflow(scala.collection.mutable.Map.empty[ActorPath, WorkflowId])

  def genEmptyLookup : Gen[WorkersToWorkflow] = oneOf(emptyLookup :: Nil)
  def genNonEmpty = for {
    m <- nonEmptyMap(genDataflows)
  } yield ActiveGoogleDataflow(m)

  implicit def arbEmptyWfStorageGenerator = Arbitrary(genEmptyWFA)
  implicit def arbEmptyLookupGenerator    = Arbitrary(genEmptyLookup)
  implicit def arbEmptyDataflows          = Arbitrary(genNonEmpty)
}


//
// State manipulation functions used by the Property testing
//
object EngineStateFunctions extends EngineStateOps2 {

  import cats._, data._, implicits._

  def addAndRemove(wfId: WorkflowId)(wrks: Set[(ActorRef,Job)]) =
    Reader{ (wfQ: ActiveWorkflows) ⇒
      addToActive(wfId)(wrks).runS(wfQ) >>= ( removeActiveWorkflowsBy(wfId).runS(_) )
    }

  def addTwiceAndRemoveOnce(wfId: WorkflowId, wfId2: WorkflowId)(wrks: Set[(ActorRef,Job)], wrks2: Set[(ActorRef,Job)]) = Reader { (wfQ: ActiveWorkflows) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>=
      ( addToActive(wfId2)(wrks2).runS(_) >>=
        (removeActiveWorkflowsBy(wfId).runS(_)) )
  }

  def addThriceAndRemoveFirst(wfId: WorkflowId, wfId2: WorkflowId, wfId3: WorkflowId)(wrks: Set[(ActorRef,Job)], wrks2: Set[(ActorRef,Job)], wrks3: Set[(ActorRef,Job)]) = Reader { (wfQ: ActiveWorkflows) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>=
      ( addToActive(wfId2)(wrks2).runS(_) >>=
        ( addToActive(wfId3)(wrks3).runS(_) >>=
          (removeActiveWorkflowsBy(wfId).runS(_))) )
  }

  def addThriceAndRemoveFirstUpdateThird(wfId: WorkflowId, wfId2: WorkflowId, wfId3: WorkflowId)(wrks: Set[(ActorRef,Job)], wrks2: Set[(ActorRef,Job)], wrks3: Set[(ActorRef,Job)]) = Reader { (wfQ: ActiveWorkflows) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>=
      ( addToActive(wfId2)(wrks2).runS(_) >>=
        ( addToActive(wfId3)(wrks3).runS(_) >>=
          (removeActiveWorkflowsBy(wfId).runS(_) >>= 
            (addToActive(wfId3)(Set((ActorRef.noSender, Job("dummy")))).runS(_)))) )
  }

}

/**
  * Reading this spec should be able to tell you how to use the State
  * operations w.r.t performing CRUD ops with State in a type safe manner
  */
object EngineStateOpsProps extends Properties("EngineState") with EngineStateOps2 with ScalaCheck {

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When initiated, 'ActiveWorkflows' should be an Empty Map container.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      activeWorkflow.map == Map()
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("After being initiated, should be non-empty when workflows are added to it.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      val (workflowId, workers) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      addToActive(workflowId)(workers).runS(activeWorkflow).value
      ACTIVE_WORKFLOWS.map.size == 1
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When adding the workflow followed by remove the same workflow from an initial workflow state i.e. 'empty'; the result structure should be still empty.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.addAndRemove
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      addAndRemove(workflowId1)(workers1)(activeWorkflow).value
      activeWorkflow.map(workflowId1).size == 0
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When adding and removing the same workflow to an empty workflow ADT twice, it would be empty.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.addAndRemove

      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 

      val workflows  = addAndRemove(workflowId1)(workers1)(activeWorkflow).value
      val workflows2 = addAndRemove(workflowId1)(workers1)(workflows).value
      !workflows2.map.contains(workflowId1) == true
      workflows2.map.size == 0
    }
  }
 
  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When adding and removing 2 workflows repeatedly, inspection of the workflow ADT will be consistent.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce}

      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 

      val workflows = addAndRemove(workflowId1)(workers1)(activeWorkflow).value
      val workflows2 = addTwiceAndRemoveOnce(workflowId1, workflowId2)(workers1, workers2)(workflows).value
      workflows2.map.size == 1
      !workflows2.map.contains(workflowId1) == true
      workflows2.map.contains(workflowId2) == true
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When repeated additions and removals is applied to the state in a specific order, inspection of the workflow ADT will be consistent.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirst}

      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      val (workflowId3, workers3) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 

      val workflows = addAndRemove(workflowId1)(workers1)(activeWorkflow).value
      !workflows.map.contains(workflowId1) == true
      !workflows.map.contains(workflowId2) == true
      !workflows.map.contains(workflowId3) == true
       workflows.map.size == 0

      val workflows2 = addTwiceAndRemoveOnce(workflowId1, workflowId2)(workers1, workers2)(workflows).value
       workflows2.map.size == 1
      !workflows2.map.contains(workflowId1) == true
       workflows2.map.contains(workflowId2) == true
      !workflows2.map.contains(workflowId3) == true

      val workflows3 = addThriceAndRemoveFirst(workflowId1, workflowId2, workflowId3)(workers1, workers2, workers3)(workflows2).value
       workflows3.map.size == 2
      !workflows3.map.contains(workflowId1) == true
       workflows3.map.contains(workflowId2) == true
       workflows3.map.contains(workflowId3) == true
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When an update is applied to a workflow ∉ of the workflow ADT, it will be like a removal followed by an insert effectively.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirst}

      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 

      addToActive(workflowId1)(workers1).runS(activeWorkflow).value
      !activeWorkflow.map.contains(workflowId1) == false
       activeWorkflow.map.size == 1
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When updates are applied to a workflow ∉ of the workflow ADT, it will be like a removal followed by an insert effectively.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirstUpdateThird}

      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 
      val (workflowId3, workers3) = (java.util.UUID.randomUUID, Set.empty[(ActorRef,Job)]) 

      val workflows = addToActive(workflowId1)(workers1).runS(activeWorkflow).value
      !workflows.map.contains(workflowId1) == false
       workflows.map.size == 1

      val workflows2 = addThriceAndRemoveFirstUpdateThird(workflowId1, workflowId2, workflowId3)(workers1, workers2, workers3)(workflows).value
      (workflows2.map.size == 2) &&
      (!workflows2.map.contains(workflowId1) == true) &&
      ( workflows2.map.contains(workflowId2) == true) &&
      (isWorkflowInActiveWorkflows(workflowId1).runA(workflows2).value == false) &&
      (workflows2.map.find(_._1 equals workflowId3).fold(false)(pair ⇒ pair._2.size == 1)) && // this should always return 'true'
      (workflows2.map.contains(workflowId3) == true)
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When updates are applied to a workflow ∉ of the workflow ADT, it will be like a removal followed by an insert effectively.") = forAll{ (activeWorkflow: ActiveWorkflows) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirstUpdateThird}

      val job1 = Job("job1")
      val job2 = Job("job2")
      val job3 = Job("job3")
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set(ActorRef.noSender -> job1)) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set(ActorRef.noSender -> job2)) 
      val (workflowId3, workers3) = (java.util.UUID.randomUUID, Set(ActorRef.noSender -> job3)) 

      val workflows = addToActive(workflowId1)(workers1).runS(activeWorkflow).value
      !workflows.map.contains(workflowId1) == false
       workflows.map.size == 1

      val workflows2 = addThriceAndRemoveFirstUpdateThird(workflowId1, workflowId2, workflowId3)(workers1, workers2, workers3)(workflows).value
      ( workflows2.map.size == 2) &&
      (!workflows2.map.contains(workflowId1) == true) &&
      ( workflows2.map.contains(workflowId2) == true) &&
      (workflows2.map.find(_._1 equals workflowId3).fold(false)(pair ⇒ pair._2.size == 1)) && // this should always return 'true'
      (workflows2.map.contains(workflowId3) == true)
    }
  }

  {
    import EngineStateOpsData.arbEmptyDataflows
    property("Binding google dataflow ids to state should be successful.") = forAll{ (dataflows: ActiveGoogleDataflow) ⇒
      import cats._, data._, implicits._

      val dataflowId1 = "DUMMY_DATAFLOW_ID1"
      val dataflowId2 = "DUMMY_DATAFLOW_ID2"
      val dataflowId3 = "DUMMY_DATAFLOW_ID3"
      val workflowId1 = java.util.UUID.randomUUID

      var t = bindDataflowToWorkflow(workflowId1)(dataflowId1).runS(dataflows).value
      t = bindDataflowToWorkflow(workflowId1)(dataflowId2).runS(t).value
      t = bindDataflowToWorkflow(workflowId1)(dataflowId3).runS(t).value

      t.map.collect{ case (k,v) if v equals workflowId1 ⇒ k }.toList.size == 3
    }
  }

  {
    import EngineStateOpsData.arbEmptyDataflows
    property("Binding google dataflow ids to state; subsequent lookups should be consistent.") = forAll{ (dataflows: ActiveGoogleDataflow) ⇒
      import cats._, data._, implicits._

      val dataflowId1 = "DUMMY_DATAFLOW_ID1"
      val dataflowId2 = "DUMMY_DATAFLOW_ID2"
      val dataflowId3 = "DUMMY_DATAFLOW_ID3"
      val workflowId1 = java.util.UUID.randomUUID

      var t = bindDataflowToWorkflow(workflowId1)(dataflowId1).runS(dataflows).value
      t = bindDataflowToWorkflow(workflowId1)(dataflowId2).runS(t).value
      t = bindDataflowToWorkflow(workflowId1)(dataflowId3).runS(t).value

      lookupDataflowBindings(workflowId1).runA(t).value.size == 3
    }
  }

  {
    import EngineStateOpsData.arbEmptyDataflows
    property("Binding google dataflow ids to state; subsequent lookups (given removals took place) should be consistent.") = forAll{ (dataflows: ActiveGoogleDataflow) ⇒
      import cats._, data._, implicits._

      val dataflowId1 = "DUMMY_DATAFLOW_ID1"
      val dataflowId2 = "DUMMY_DATAFLOW_ID2"
      val dataflowId3 = "DUMMY_DATAFLOW_ID3"
      val workflowId1 = java.util.UUID.randomUUID

      var t = bindDataflowToWorkflow(workflowId1)(dataflowId1).runS(dataflows).value
      t = bindDataflowToWorkflow(workflowId1)(dataflowId2).runS(t).value
      t = bindDataflowToWorkflow(workflowId1)(dataflowId3).runS(t).value

      lookupDataflowBindings(workflowId1).runA(t).value.size == 3
      t = removeFromDataflowBindings(workflowId1).runS(t).value
      t.map.size == dataflows.map.size
    }
  }

}

