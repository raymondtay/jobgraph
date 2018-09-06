package hicoden.jobgraph

import hicoden.jobgraph.configuration.workflow.model.WorkflowConfig
import hicoden.jobgraph.configuration.step.model.JobConfig

import quiver.{Graph ⇒ QGraph, LNode, LEdge, mkGraph}
import com.typesafe.scalalogging.Logger

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
  val inactive,           /* Jobs start in the inactive state */
      start,              /* Jobs are bootstrapped but not directly executing would land in this state */
      active,             /* Jobs once started would land in the active state */
      updated,            /* Jobs were updated, jobgraph ignores this state */
      forced_termination, /* Jobs cancelled by hand or automatic means would land in this state */
      finished,           /* Jobs might terminate normally in the finished state */
      failed,             /* Jobs might terminate abnormally in the failed state */
      unknown = Value     /* Jobs might end up in the unknown state */
}

// All workflows start at the 'not_started' state, then it would progress to
// the 'started' state when at least 1 of its jobs have started and if it was
// stopped then the state would enter `forced_termination` otherwise it would
// enter `finished` (note: in 'finished' state, jobgraph does not distinguished
// between OK/Failed runs)
object WorkflowStates extends Enumeration {
  type States = Value
  val not_started, /* workflows, when first bootstrapped would land in this state */
      started,     /* workflows, when started but hasn't completed would land in this state */
      finished,    /* workflows, when run to full completion or abnormal termination would land in this state */
      forced_termination = Value /* Workflows cancelled by hand or automatic means would land in this state */
}


case class Workflow(jobgraph: QGraph[Job,UUID,String],
                    config: WorkflowConfig = null,
                    create_timestamp : java.time.Instant = Instant.now(),
                    id : WorkflowId = UUID.randomUUID,
                    var status : WorkflowStates.States = WorkflowStates.not_started)

case class Job(name: String,
               config: JobConfig = null,
               create_timestamp : java.time.Instant = Instant.now(),
               id : JobId = UUID.randomUUID,
               var state : JobStates.States = JobStates.inactive)

trait WorkflowImplicits {
  implicit val orderByCreationTime = new Ordering[Workflow] {
    def compare(a: Workflow, b: Workflow) =
      if (a.create_timestamp equals b.create_timestamp) 0 else
      if (a.create_timestamp isBefore b.create_timestamp) 1 else -1
  }
}

trait WorkflowOps extends WorkflowImplicits {
  import cats._, data._, implicits._

  val work : collection.mutable.PriorityQueue[Workflow]

  // TODO: Need to accumulate logs? Might need to think about Writers
  //
  // Logger is not an aggregator but if we do need one, we need a Monoidal
  // Functor something like a Writer Monad
  val logger = Logger(classOf[WorkflowOps])

  /**
    * Creates a multi-graph model based on the inputs; see [[quiver.Graph]] for
    * dtails. Once the parsing of the configuration file is completed, then its
    * likely that you would invoke this function.
    * 
    * @param wfConfig some kind of workflow config
    * @param nodes
    * @param edges
    * @return multigraph
    */
  def createWf(wfConfig: Option[WorkflowConfig], nodes: Seq[LNode[Job,UUID]]) : Reader[Seq[LEdge[Job,String]], Workflow] = Reader {
    (edges: Seq[LEdge[Job,String]]) ⇒
      wfConfig.fold{
        val wf = Workflow(jobgraph = mkGraph(nodes, edges))
        work += wf
        wf }{config ⇒
        val wf = Workflow(jobgraph = mkGraph(nodes, edges), config = config)
        work += wf
        wf}
  }

  /**
    * Find the start node and set the state to 'start'.
    * @param wf - a workflow [[Workflow]]
    * @param node - a node [[LNode]] and [[Job]]
    * @return returns a collection of jobs to be started by an external
    * executioner
    */
  def startWorkflow : Reader[WorkflowId, Option[Set[Job]]] = Reader { (wfId: WorkflowId) ⇒
    work.find(_.id equals wfId).fold[Option[Set[Job]]](none){ workflow ⇒
      if (workflow.jobgraph.hasLoop) {
        logger.info("[Workflow] loops detected for workflow: {}.", workflow.id)
        none
      } else{
        val startNodes = workflow.jobgraph.roots.map(node ⇒ updateNodeState(JobStates.start)(node))
        logger.info("[Workflow] Node(s) for workflow: {} have been updated to {}.", workflow.id, JobStates.start)
        startNodes.some
      }
    }
  }

  /**
    * Update the step in the workflow with the given state
    * @param wfId - ID of the workflow
    * @param jobId - ID of the job or step
    * @param state - to be updated
    */
  def updateWorkflow(wfId: WorkflowId)(node: JobId) : Reader[JobStates.States, Either[Exception, Option[Boolean]]] = Reader{ (state: JobStates.States) ⇒
    work.find(_.id equals wfId).fold[Either[Exception, Option[Boolean]]](Left(new Exception(s"[Workflow][Update] Did not find workflow matching id: $wfId"))){ workflow ⇒
      val updatedNodes = workflow.jobgraph.nodes.filter(_.id equals node).map(updateNodeState(state)(_))
      Either.cond(
        !updatedNodes.isEmpty,
        true.some,
        new Exception("[Workflow][Update] Found workflow but matching job was not discovered. Weird.")
      )
    }
  }

  /**
    * Stops the workflow by setting the state of the entire workflow to
    * forced_termination
    * @param wfId - workflow id
    * @return either a [[Either.Left]] value to indicate what went wrong or a
    * [[Either.Right]] to indicate a success (the payload carried is either an empty container or a list of updated nodes)
    */
  def stopWorkflow : Reader[WorkflowId, Either[String, List[Job]]] = Reader{ (wfId: WorkflowId) ⇒
    work.find(_.id equals wfId).fold[Either[String, List[Job]]](Left(s"Cannot discover workflow of the id: $wfId")){ workflow ⇒
      val updatedNodes = workflow.jobgraph.labNodes.map(labeledNode ⇒ updateNodeState(JobStates.forced_termination)(labeledNode.vertex))
      if (updatedNodes.isEmpty) { List.empty[Job].asRight } else { updatedNodes.toList.asRight }
    }
  }

  /**
    * Traverses the given workflow (via [[wfId]]) and attempts to discover the
    * next nodes to start. See [[Engine]] on how its being used
    * @param wfId
    * @param jobId
    * @param either a [[Either.Left]] value to indicate that the workflow count
    * not be located in the internal ADT or a [[Either.Right]] carrying the
    * payload which we are interested in.
    */
  def discoverNextJobsToStart(wfId: WorkflowId) : Reader[JobId, Either[String, Vector[Job]]] = Reader{ (jobId: JobId) ⇒
    work.find(_.id equals wfId).fold[Either[String, Vector[Job]]](Left(s"Cannot discover workflow of the id: $wfId")){
      workflow ⇒
        val ancestorsByGroup : Vector[Seq[Job]] =
          workflow.jobgraph.labfilter(_ equals jobId).nodes.
            map(n ⇒ workflow.jobgraph.successors(n)).
            flatten.map(x ⇒ workflow.jobgraph.rdfs(x::Nil))

        ancestorsByGroup.map(group ⇒ group.filter(node ⇒ node.state == JobStates.inactive)).flatten.asRight
    }
  }

  /**
    * When the given workflow (i.e. [[wfId]]) is active, this procedure discover the next batch of
    * work with the requirement that for each successor of the given job-id
    * (i.e. [[jobId]]) to be part of the next batch, than all predecessors must be in
    * the state FINISHED state. That is to say, if the previous work is not
    * completed normally or has not even started, that this node is omitted.
    * @param wfId workflow id
    * @param jobId job id
    * @return An empty container or some container that contains the next batch
    */
  def discoverNext(wfId: WorkflowId) : Reader[JobId, Either[String, Vector[Job]]] = Reader{ (jobId: JobId) ⇒
    work.find(_.id equals wfId).fold[Either[String, Vector[Job]]](Left(s"Cannot discover workflow of the id: $wfId")){
      workflow ⇒
        val succToPreds : Vector[Vector[(Job, Vector[Job])]] = {
          val subGraph = workflow.jobgraph.labfilter(_ equals jobId)
          if(!subGraph.hasLoop) {
            subGraph.nodes.
            map(n ⇒ workflow.jobgraph.successors(n).map(x ⇒ (x, workflow.jobgraph.predecessors(x))))
          } else Vector.empty[Vector[(Job,Vector[Job])]]
        }

        val result : Vector[(Job, Vector[Job])] =
          succToPreds.map(xs ⇒ xs.filter(pair ⇒ pair._2.forall(_.state == JobStates.finished))).flatten

        result.foldLeft(Vector.empty[Job])((acc, e) ⇒ acc :+ e._1).asRight
    }
  }

  /**
    * Updates the state of the step/job; note that the state is overriden with
    * the incoming state value; no check is done.
    * @param state
    * @param node
    * @return the updated node
    */
  def updateNodeState(state: JobStates.States) : Reader[Job, Job] = Reader {(node: Job) ⇒
    node.state = state
    node
  }

  /**
    * Looksup the workflow in the internal storage and exports that information
    * out 
    * @param wfId
    * @return Some(WorkflowStatus) or None
    */
  def getWorkflowStatus : Reader[WorkflowId, Option[WorkflowStatus]] = Reader{(wfId: WorkflowId) ⇒
    def render(job: Job) = JobStatus(id = job.id, status = job.state)

    work.find(_.id equals wfId).fold(none[WorkflowStatus]){
      workflow ⇒
      WorkflowStatus(createTime = workflow.create_timestamp,
                     status     = if (isWorkflowStarted(workflow) && !isWorkflowDone(workflow)) WorkflowStates.started
                                  else if (isWorkflowStarted(workflow) && isWorkflowDone(workflow)) WorkflowStates.finished
                                  else WorkflowStates.not_started,
                     steps      = workflow.jobgraph.nodes.map(render(_)).toList).some
    } 
  }

  /**
    * Check whether all job's have reached the normal completion state
    * @param wfId
    * @return true if all jobs have reached normal completion state; else false
    */
  def isWorkflowCompleted = Reader{ (wfId: WorkflowId) ⇒
    work.find(_.id equals wfId).fold(false){
      workflow ⇒ isWorkflowCompletedOK(workflow)
    }
  }

  /**
    * Check whether any jobs have reached an abnormal completion state
    * @param wfId
    * @return true if any jobs are terminated abnormally; else false
    */
  def isWorkflowForcedStop = Reader{ (wfId: WorkflowId) ⇒
    work.find(_.id equals wfId).fold(false){
      workflow ⇒ isWorkflowCompletedNOK(workflow)
    }
  }

  // As long as ANY of the root nodes ∉ "inactive", then its considered
  // started.
  private[jobgraph]
  def isWorkflowStarted = Reader{ (workflow: Workflow) ⇒
    val inactiveNodes = workflow.jobgraph.roots.filter(node ⇒ node.state equals JobStates.inactive )
    if (inactiveNodes.size == workflow.jobgraph.roots.size) false else true
  }

  // A workflow is considered "done" iff all nodes entered forced termination
  // or finished
  private[jobgraph]
  def isWorkflowDone = Reader{ (workflow: Workflow) ⇒
    val doneNodes =
      workflow.jobgraph.nodes.filter(
        node ⇒
          (node.state equals JobStates.forced_termination) ||
          (node.state equals JobStates.finished)
        )
    if (doneNodes.isEmpty) false else
    if (doneNodes.size == workflow.jobgraph.nodes.size) true else false
  }

  private[jobgraph]
  def isWorkflowCompletedOK = Reader{ (workflow: Workflow) ⇒
    val doneNodes =
      workflow.jobgraph.nodes.filter(
        node ⇒ node.state equals JobStates.finished
        )
    if (doneNodes.isEmpty) false else
    if (doneNodes.size == workflow.jobgraph.nodes.size) true else false
  }

  private[jobgraph]
  def isWorkflowCompletedNOK = Reader{ (workflow: Workflow) ⇒
    val nodes =
      workflow.jobgraph.nodes.filter(
        node ⇒
          node.state equals JobStates.forced_termination
        )
    if (nodes.isEmpty) false else true
  }

}

object WorkflowOps extends WorkflowOps {
  override val work : collection.mutable.PriorityQueue[Workflow] =
    collection.mutable.PriorityQueue.empty[Workflow]
}

