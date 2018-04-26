package hicoden.jobgraph

import quiver.{Graph => QGraph, LNode, LEdge, mkGraph}
import com.typesafe.scalalogging._

// scala language imports
import scala.language.postfixOps

// java imports
import java.time.Instant
import java.util.UUID

/**
  * The intention is to model a "job" or "step" which is being used
  * interchangably over here.
  *
  * The story is that the steps are described in HOCON syntax and the
  * validation process will determine if the configuration format went awry.
  *
  * ANY workflow, when first created, is not active til the "start" signal and
  * is given a timestamp to mark the creation time.
  *
  */

object JobStates extends Enumeration {
  type States = Value
  val inactive, start, active, forced_termination, finished = Value
}

sealed trait Step {
  private[jobgraph] var state = JobStates.inactive
}

case class Workflow(jobgraph: QGraph[Job,String,String]) extends Step {
  private[jobgraph] val create_timestamp : java.time.Instant = Instant.now()
  private[jobgraph] val id : WorkflowId = UUID.randomUUID
}

//
// TODO: Attributes ???? Need'em ?
//
case class Job(name: String) extends Step

trait WorkflowImplicits {
  implicit val orderByCreationTime = new Ordering[Workflow] {
    def compare(a: Workflow, b: Workflow) =
      if (a.create_timestamp equals b.create_timestamp) 0 else
      if (a.create_timestamp isBefore b.create_timestamp) 1 else -1
  }
}

class WorkflowOps extends WorkflowImplicits {
  import cats._, data._, implicits._

  private[this] val work : collection.mutable.PriorityQueue[Workflow] = collection.mutable.PriorityQueue.empty[Workflow] // Internal data structure to manage the work items 

  // Logger is not an aggregator but if we do need one, we need a Monoidal
  // Functor something like a Writer Monad
  val logger = Logger(classOf[WorkflowOps])

  /**
    * Creates a multi-graph model based on the inputs; see [[quiver.Graph]] for
    * dtails. Once the parsing of the configuration file is completed, then its
    * likely that you would invoke this function.
    *
    * @param nodes
    * @param edges
    * @return multigraph
    */
  def createWf(nodes: Seq[LNode[Job,String]]) = Reader {
    (edges: Seq[LEdge[Job,String]]) ⇒
      val wf = Workflow(mkGraph(nodes, edges))
      work += wf
      wf
  }

  /**
    * Find the start node and set the state to 'start'.
    * @param wf - a workflow [[Workflow]]
    * @param node - a node [[LNode]] and [[Job]]
    * @return returns a collection of jobs to be started by an external
    * executioner
    */
  def startWf : Reader[WorkflowId, Option[Set[Job]]] = Reader { (wfId: WorkflowId) ⇒
    work.find(_.id equals wfId).fold[Option[Set[Job]]](none){ workflow ⇒
      val startNodes = workflow.jobgraph.roots.map(node ⇒ updateNodeState(JobStates.start)(node))
      logger.info("[Workflow] Node(s) for workflow: {} have been updated to {}.", workflow.id, JobStates.start)
      startNodes.some
    }
  }

  /**
    * Update the step in the workflow with the given state
    * @param wfId - ID of the workflow
    * @param jobId - ID of the job or step
    * @param state - to be updated
    */
  def updateWorkflow(wfId: WorkflowId)(node: JobId) : Reader[JobStates.States, Option[Boolean]] = Reader{ (state: JobStates.States) ⇒
    work.find(_.id equals wfId).fold[Option[Boolean]](none){ workflow ⇒
      val updated = workflow.jobgraph.nodes.filter(_ equals node).map(updateNodeState(state)(_))
      true.some
    }
  }

  /**
    * Stops the workflow by setting the state of the entire workflow to
    * forced_termination
    * @param wfId - workflow id
    * @return either a [[Either.Left]] value to indicate what went wrong or a
    * [[Either.Right]] to indicate a success
    */
  def stopWorkflow = Reader{ (wfId: WorkflowId) ⇒
    work.find(_.id equals wfId).fold[Either[String, Option[Boolean]]](Left(s"Cannot discover workflow of the id: $wfId")){ workflow ⇒
      Right(true.some)
    }
  }

  /**
    * Updates the state of the step/job; note that the state is overriden with
    * the incoming state value; no check is done.
    * @param state
    * @param node
    * @return the updated node
    */
  def updateNodeState(state: JobStates.States) = Reader {(node: Job) ⇒ 
    node.state = state
    node
  }

  def displayWfState = Reader { (wf: Workflow) ⇒
    val repr = wf.jobgraph
    logger.info(s"""
      [Workflow][display]
      $repr
      """)
  }

}

object WorkflowOps extends WorkflowOps

