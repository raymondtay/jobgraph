package hicoden.jobgraph.examples

/**
  * A simple Digraph
  */
object DiGraph extends App {
  import hicoden.jobgraph._
  import quiver._

  val jobA = Job("job-a")
  val jobB = Job("job-b")
  val jobC = Job("job-c")
  val jobD = Job("job-d")

  val node1 = LNode(jobA, "job-a")
  val node2 = LNode(jobB, "job-b")
  val node3 = LNode(jobC, "job-c")
  val node4 = LNode(jobD, "job-d")

  val e1 = LEdge(jobA, jobB, "a -> b")
  val e2 = LEdge(jobA, jobC, "a -> c")
  val e3 = LEdge(jobC, jobD, "c -> d")
  val e4 = LEdge(jobB, jobD, "b -> d")

  import WorkflowOps._

  val workflow = createWf(collection.immutable.Seq(node1, node2, node3, node4))(collection.immutable.Seq(e1, e2, e3, e4))
  startWf(workflow.id)
  displayWfState(workflow)

}
