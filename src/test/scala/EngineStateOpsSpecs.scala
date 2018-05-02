package hicoden.jobgraph.engine

import hicoden.jobgraph.{WorkflowId, Workflow}

import akka.actor.ActorRef
import org.specs2._
import org.scalacheck._
import com.typesafe.config._
import Arbitrary._
import Gen.{containerOfN, choose, pick, mapOf, listOf, listOfN, oneOf}
import Prop.{forAll, throws, AnyOperators}

//
// The type WFA is the typed synonym for active workflows
// The type WFI is the typed synonym for idle workflows
//
object EngineStateOpsData {

  def emptyActiveWF : WFA = scala.collection.mutable.Map.empty[WorkflowId, Set[ActorRef]]

  def genEmptyWFA : Gen[WFA] = oneOf(emptyActiveWF :: Nil)

  implicit def arbEmptyWfStorageGenerator = Arbitrary(genEmptyWFA)
}


//
// State manipulation functions used by the Property testing
//
object EngineStateFunctions extends EngineStateOps {

  import cats._, data._, implicits._

  def addAndRemove(wfId: WorkflowId)(wrks: Set[ActorRef]) = Reader{ (wfQ: WFA) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>= ( removeFromActive(wfId).runS(_) )
  }

  def addTwiceAndRemoveOnce(wfId: WorkflowId, wfId2: WorkflowId)(wrks: Set[ActorRef], wrks2: Set[ActorRef]) = Reader { (wfQ: WFA) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>=
      ( addToActive(wfId2)(wrks2).runS(_) >>=
        (removeFromActive(wfId).runS(_)) )
  }

  def addThriceAndRemoveFirst(wfId: WorkflowId, wfId2: WorkflowId, wfId3: WorkflowId)(wrks: Set[ActorRef], wrks2: Set[ActorRef], wrks3: Set[ActorRef]) = Reader { (wfQ: WFA) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>=
      ( addToActive(wfId2)(wrks2).runS(_) >>=
        ( addToActive(wfId3)(wrks3).runS(_) >>=
          (removeFromActive(wfId).runS(_))) )
  }

  def addThriceAndRemoveFirstUpdateThird(wfId: WorkflowId, wfId2: WorkflowId, wfId3: WorkflowId)(wrks: Set[ActorRef], wrks2: Set[ActorRef], wrks3: Set[ActorRef]) = Reader { (wfQ: WFA) ⇒
    addToActive(wfId)(wrks).runS(wfQ) >>=
      ( addToActive(wfId2)(wrks2).runS(_) >>=
        ( addToActive(wfId3)(wrks3).runS(_) >>=
          (removeFromActive(wfId).runS(_) >>= 
            (updateActive(wfId3)(Set(ActorRef.noSender)).runS(_)))) )
  }

}

/**
  * Reading this spec should be able to tell you how to use the State
  * operations w.r.t performing CRUD ops with State in a type safe manner
  */
object EngineStateOpsProps extends Properties("EngineState") with EngineStateOps with ScalaCheck {

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When initiated, should be empty.") = forAll{ (activeWorkflow: WFA) ⇒
      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("After being initiated, should be non-empty when workflows are added to it.") = forAll{ (activeWorkflow: WFA) ⇒
      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
      val (workflowId, workers) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val workflows = addToActive(workflowId)(workers).runS(activeWorkflow).value
      workflows.size == 1
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When adding the workflow followed by remove the same workflow from an initial workflow state i.e. 'empty'; the result structure should be still empty.") = forAll{ (activeWorkflow: WFA) ⇒
      import cats._, data._, implicits._
      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val workflows = (addToActive(workflowId1)(workers1).runS(activeWorkflow) >>= ((newWorkflow: WFA) ⇒ removeFromActive(workflowId1).runS(newWorkflow))).value
      workflows.size == 0
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When adding and removing the same workflow to an empty workflow ADT twice, it would be empty.") = forAll{ (activeWorkflow: WFA) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.addAndRemove

      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 

      val workflows : WFA = addAndRemove(workflowId1)(workers1)(activeWorkflow).value
      val workflows2 : WFA = addAndRemove(workflowId1)(workers1)(workflows).value
      !workflows2.contains(workflowId1) == true
      workflows2.size == 0
    }
  }
 
  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When adding and removing 2 workflows repeatedly, inspection of the workflow ADT will be consistent.") = forAll{ (activeWorkflow: WFA) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce}

      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 

      val workflows : WFA = addAndRemove(workflowId1)(workers1)(activeWorkflow).value

      val workflows2 : WFA = addTwiceAndRemoveOnce(workflowId1, workflowId2)(workers1, workers2)(workflows).value
      workflows2.size == 1
      !workflows2.contains(workflowId1) == true
      workflows2.contains(workflowId2) == true
    }
  }
 
  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When repeated additions and removals is applied to the state in a specific order, inspection of the workflow ADT will be consistent.") = forAll{ (activeWorkflow: WFA) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirst}

      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val (workflowId3, workers3) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 

      val workflows : WFA = addAndRemove(workflowId1)(workers1)(activeWorkflow).value
      !workflows.contains(workflowId1) == true
      !workflows.contains(workflowId2) == true
      !workflows.contains(workflowId3) == true
      workflows.size == 0

      val workflows2 : WFA = addTwiceAndRemoveOnce(workflowId1, workflowId2)(workers1, workers2)(workflows).value
      workflows2.size == 1
      !workflows2.contains(workflowId1) == true
      workflows2.contains(workflowId2) == true
      !workflows2.contains(workflowId3) == true

      val workflows3 : WFA = addThriceAndRemoveFirst(workflowId1, workflowId2, workflowId3)(workers1, workers2, workers3)(workflows2).value
      workflows3.size == 2
      !workflows3.contains(workflowId1) == true
      workflows3.contains(workflowId2) == true
      workflows3.contains(workflowId3) == true
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When an update is applied to a workflow ∉ of the workflow ADT, it will be like a removal followed by an insert effectively.") = forAll{ (activeWorkflow: WFA) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirst}

      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()
      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 

      val workflows : WFA = updateActive(workflowId1)(workers1).runS(activeWorkflow).value
      !workflows.contains(workflowId1) == false
      workflows.size == 1
    }
  }

  {
    import EngineStateOpsData.arbEmptyWfStorageGenerator
    property("When updates are applied to a workflow ∉ of the workflow ADT, it will be like a removal followed by an insert effectively.") = forAll{ (activeWorkflow: WFA) ⇒
      import cats._, data._, implicits._
      import EngineStateFunctions.{addAndRemove, addTwiceAndRemoveOnce, addThriceAndRemoveFirstUpdateThird}

      getCurrentActiveWorkflows.runS(activeWorkflow).value == Map()

      val (workflowId1, workers1) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val (workflowId2, workers2) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 
      val (workflowId3, workers3) = (java.util.UUID.randomUUID, Set.empty[ActorRef]) 

      val workflows : WFA = updateActive(workflowId1)(workers1).runS(activeWorkflow).value
      !workflows.contains(workflowId1) == false
      workflows.size == 1

      val workflows2 : WFA = addThriceAndRemoveFirstUpdateThird(workflowId1, workflowId2, workflowId3)(workers1, workers2, workers3)(workflows).value
      workflows2.size == 2
      !workflows2.contains(workflowId1) == true
      workflows2.contains(workflowId2) == true
      workflows2.find(_._1 equals workflowId3).fold(false)(pair ⇒ pair._2.size == 1) // this should always return 'true'
      workflows2.contains(workflowId3) == true

    }
  }
 
}
